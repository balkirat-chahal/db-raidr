using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using MySqlCdc;
using MySqlCdc.Events;
using MySqlConnector;
using CdcSslMode = MySqlCdc.Constants.SslMode;

namespace DbRaidr;

/// <summary>
/// Startup: check MySQL replication settings, load column kinds, open the
/// binlog client, and yield events while tracking file/position ourselves.
/// </summary>
internal sealed partial class Migration
{
    /// <summary>
    /// Bad settings fail late and confusingly: MINIMAL row images write rows
    /// full of missing values, and PARTIAL_JSON logs JSON diffs that wipe out
    /// the rest of the document when replayed.
    /// </summary>
    private async Task CheckSettingsAsync()
    {
        var wanted = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["binlog_format"] = "ROW",
            ["binlog_row_image"] = "FULL",
            ["binlog_row_metadata"] = "FULL",
            ["binlog_row_value_options"] = ""
        };

        foreach ((string name, string expected) in wanted)
        {
            await using var command = _mysql.CreateCommand();
            command.CommandText = "SELECT @@GLOBAL." + name + " AS v";
            object? raw = await command.ExecuteScalarAsync(_forceToken);
            string actual = Convert.ToString(raw is DBNull ? null : raw,
                CultureInfo.InvariantCulture)?.ToUpperInvariant() ?? string.Empty;
            if (!string.Equals(actual, expected, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"MySQL {name} is '{actual}', must be '{expected}'");
            }
        }
    }

    /// <summary>
    /// Ask MySQL what each column really is. Trusting a hand-written config
    /// means one missed BLOB gets decoded as UTF-8 and stored as mojibake.
    /// </summary>
    private async Task LoadKindsAsync(Table table)
    {
        await using var command = _mysql.CreateCommand();
        command.CommandText =
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, IS_NULLABLE, " +
            "COLUMN_TYPE, ORDINAL_POSITION " +
            "FROM information_schema.COLUMNS " +
            "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table " +
            "ORDER BY ORDINAL_POSITION";
        command.Parameters.AddWithValue("@schema", _db);
        command.Parameters.AddWithValue("@table", table.MySqlTable);

        var rows = new List<Dictionary<string, object?>>();
        await using (var reader = await command.ExecuteReaderAsync(_forceToken))
        {
            while (await reader.ReadAsync(_forceToken))
            {
                var row = new Dictionary<string, object?>(StringComparer.Ordinal);
                for (int i = 0; i < reader.FieldCount; i++)
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }
        }

        if (rows.Count == 0)
            throw new InvalidOperationException(table.MySqlTable + ": no such table in " + _db);

        foreach (Dictionary<string, object?> row in rows)
        {
            string name = Convert.ToString(row["COLUMN_NAME"], CultureInfo.InvariantCulture)!;
            string dataType = Convert.ToString(row["DATA_TYPE"], CultureInfo.InvariantCulture)!.ToLowerInvariant();
            string charset = Convert.ToString(row["CHARACTER_SET_NAME"], CultureInfo.InvariantCulture)?.ToLowerInvariant()
                             ?? string.Empty;
            string columnType = Convert.ToString(row["COLUMN_TYPE"], CultureInfo.InvariantCulture) ?? dataType;

            string kind;
            if (dataType is "json" or "bit" or "set")
                kind = dataType;
            else if (dataType == "enum")
                kind = "enum";      // MySqlCdc uses the numeric enum index.
            else if (dataType == "decimal")
                kind = "decimal";   // Preserve arbitrary MySQL DECIMAL precision as text.
            else if (BinaryTypes.Contains(dataType) || charset == "binary")
                kind = "binary";
            else
                kind = "plain";

            IReadOnlyList<string> members = dataType is "set" or "enum"
                ? ParseTypeMembers(columnType)
                : [];

            table.AddColumn(new ColumnInfo
            {
                Name = name,
                Kind = kind,
                SourceType = dataType,
                Unsigned = columnType.Contains(" unsigned", StringComparison.OrdinalIgnoreCase),
                TypeMembers = members
            });

            // (key) > (last key) is never true when a key value is NULL, so those
            // rows would be returned by no chunk at all.
            string nullable = Convert.ToString(row["IS_NULLABLE"], CultureInfo.InvariantCulture) ?? "";
            if (table.KeyColumns.Contains(name, StringComparer.Ordinal) && nullable == "YES")
                throw new InvalidOperationException(
                    table.MySqlTable + ": key column " + name + " must be NOT NULL");
        }

        foreach (string column in table.KeyColumns)
            _ = table.Column(column); // throws if the configured key column does not exist
    }

    /// <summary>
    /// Parse information_schema COLUMN_TYPE such as:
    ///     set('a','b') / enum('one','two')
    /// including ordinary MySQL backslash escapes and doubled quotes.
    /// </summary>
    private static IReadOnlyList<string> ParseTypeMembers(string columnType)
    {
        int open = columnType.IndexOf('(');
        int close = columnType.LastIndexOf(')');
        if (open < 0 || close <= open)
            return [];

        var result = new List<string>();
        var current = new StringBuilder();
        bool inString = false;

        for (int i = open + 1; i < close; i++)
        {
            char c = columnType[i];
            if (!inString)
            {
                if (c == '\'')
                {
                    inString = true;
                    current.Clear();
                }
                continue;
            }

            if (c == '\\' && i + 1 < close)
            {
                char escaped = columnType[++i];
                current.Append(escaped switch
                {
                    '0' => '\0',
                    'b' => '\b',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'Z' => (char)26,
                    _ => escaped
                });
                continue;
            }

            if (c == '\'')
            {
                if (i + 1 < close && columnType[i + 1] == '\'')
                {
                    current.Append('\'');
                    i++;
                    continue;
                }

                result.Add(current.ToString());
                inString = false;
                continue;
            }

            current.Append(c);
        }

        return result;
    }

    /// <summary>
    /// A blocking reader is required: a non-blocking reader stops as soon as
    /// it drains, so the loop waiting for a high watermark exits early.
    /// Heartbeats keep the iterator waking up on an idle server.
    /// </summary>
    private async Task OpenStreamAsync()
    {
        if (string.IsNullOrEmpty(_state.LogFile))
        {
            Dictionary<string, object?>? status;
            try
            {
                status = await ReadSingleRowAsync("SHOW BINARY LOG STATUS"); // MySQL 8.4+
            }
            catch (MySqlException)
            {
                status = await ReadSingleRowAsync("SHOW MASTER STATUS");
            }

            if (status is null)
                throw new InvalidOperationException("MySQL did not return a binary log position");

            // Start at the end of the log: the backfill picks up everything already
            // in the tables, so there is no history worth replaying.
            _state.LogFile = Convert.ToString(status["File"], CultureInfo.InvariantCulture);
            _state.LogPos = Convert.ToInt64(status["Position"], CultureInfo.InvariantCulture);
            _state.Save();
        }

        _currentLogFile = _state.LogFile;
        _currentLogPos = _state.LogPos;
        Log.Info("stream at {0}:{1}", _state.LogFile ?? "",
            _state.LogPos?.ToString(CultureInfo.InvariantCulture) ?? "");

        _stream = new BinlogClient(options =>
        {
            options.Hostname = _config.MySql.Host;
            options.Port = _config.MySql.Port == 0 ? 3306 : _config.MySql.Port;
            options.Username = _config.MySql.User;
            options.Password = _mysqlPassword;
            options.Database = _db;
            options.ServerId = _config.ServerId;
            options.Blocking = true;
            options.SslMode = CdcSslMode.Disabled;
            options.HeartbeatInterval = TimeSpan.FromSeconds(_config.HeartbeatSeconds);
            options.Binlog = BinlogOptions.FromPosition(_state.LogFile!, _state.LogPos!.Value);
        });

        await Task.CompletedTask;
    }

    private async Task<Dictionary<string, object?>?> ReadSingleRowAsync(string sql)
    {
        await using var command = _mysql.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(_forceToken);
        if (!await reader.ReadAsync(_forceToken))
            return null;

        var row = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        return row;
    }

    /// <summary>
    /// Yield binlog events while tracking file and position ourselves.
    /// MySqlCdc updates client.State only after control returns from the
    /// yielded event. Track the position first so a checkpoint taken while
    /// applying XidEvent records the next-event position.
    /// </summary>
    private async IAsyncEnumerable<IBinlogEvent> StreamEventsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_stream is null)
            throw new InvalidOperationException("binlog stream is not open");

        await foreach ((EventHeader header, IBinlogEvent binlogEvent) in
                       _stream.Replicate(cancellationToken).WithCancellation(cancellationToken))
        {
            if (binlogEvent is RotateEvent rotate)
            {
                _currentLogFile = rotate.BinlogFilename;
                _currentLogPos = rotate.BinlogPosition;
            }
            else if (header.NextEventPosition > 0)
            {
                _currentLogPos = header.NextEventPosition;
            }

            if (binlogEvent is TableMapEvent tableMap)
                _tableMaps[tableMap.TableId] = tableMap;

            yield return binlogEvent;
        }
    }
}
