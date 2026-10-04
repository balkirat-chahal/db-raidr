using Npgsql;

namespace DbRaidr;

/// <summary>
/// PostgreSQL writes. Every path is idempotent: upsert by key, delete by key,
/// so replaying a few events after a crash is harmless.
/// </summary>
internal sealed partial class Migration
{
    /// <summary>
    /// Commit PostgreSQL first, record the position second, so the saved
    /// position can only lag the data. Lagging just replays idempotent writes.
    /// </summary>
    private async Task CheckpointAsync()
    {
        await _pgTransaction.CommitAsync(_forceToken);
        await _pgTransaction.DisposeAsync();
        _pgTransaction = await _postgres.BeginTransactionAsync(_forceToken);

        if (_stream is not null)
        {
            _state.LogFile = _currentLogFile;
            _state.LogPos = _currentLogPos;
        }
        _state.Save();
    }

    /// <summary>
    /// Bulk INSERT ... ON CONFLICT DO UPDATE the given rows into PostgreSQL.
    /// Idempotent by design, so replaying after crash-and-resume is harmless.
    /// </summary>
    private async Task UpsertAsync(Table table, List<Dictionary<string, object?>> rows)
    {
        if (rows.Count == 0)
            return;

        List<string> columns = rows[0].Keys.OrderBy(value => value, StringComparer.Ordinal).ToList();
        List<string> sets = columns
            .Where(column => !table.KeyColumns.Contains(column, StringComparer.Ordinal))
            .Select(column => SqlNames.QuotePg(column) + " = EXCLUDED." + SqlNames.QuotePg(column))
            .ToList();
        string action = sets.Count > 0 ? "DO UPDATE SET " + string.Join(", ", sets) : "DO NOTHING";
        string head = "INSERT INTO " + SqlNames.QuotePg(table.PgTable) + " (" +
                      string.Join(", ", columns.Select(SqlNames.QuotePg)) + ") VALUES ";
        string tail = " ON CONFLICT (" +
                      string.Join(", ", table.KeyColumns.Select(SqlNames.QuotePg)) + ") " + action;

        // Pages of 100 rows per INSERT statement.
        for (int start = 0; start < rows.Count; start += 100)
        {
            List<Dictionary<string, object?>> page = rows.Skip(start).Take(100).ToList();
            var valueRows = new List<string>(page.Count);
            var parameters = new List<object?>();
            int parameterNumber = 1;

            foreach (Dictionary<string, object?> row in page)
            {
                var placeholders = new List<string>(columns.Count);
                foreach (string column in columns)
                {
                    string placeholder = "$" + parameterNumber++;
                    placeholder = CastPlaceholderForPostgres(placeholder, table.Column(column));
                    placeholders.Add(placeholder);
                    parameters.Add(row.TryGetValue(column, out object? value) ? value : null);
                }
                valueRows.Add("(" + string.Join(", ", placeholders) + ")");
            }

            await using var command = new NpgsqlCommand(head + string.Join(", ", valueRows) + tail,
                _postgres, _pgTransaction);
            foreach (object? value in parameters)
                command.Parameters.Add(new NpgsqlParameter { Value = value ?? DBNull.Value });
            await command.ExecuteNonQueryAsync(_forceToken);
        }
    }

    /// <summary>
    /// Npgsql sends strings as PostgreSQL text. Explicit casts send
    /// arbitrary-precision DECIMAL and JSON text to the intended types.
    /// </summary>
    private static string CastPlaceholderForPostgres(string placeholder, ColumnInfo info)
    {
        return info.Kind switch
        {
            "decimal" => placeholder + "::numeric",
            "json" => placeholder + "::jsonb",
            _ => placeholder
        };
    }

    /// <summary>
    /// DELETE the row matching this table's key columns. Deleting a key that is
    /// already gone is a no-op, so this is safe to replay too.
    /// </summary>
    private async Task DeleteAsync(Table table, Dictionary<string, object?> row, bool fromBinlog)
    {
        var where = new List<string>();
        var values = new List<object?>();
        int number = 1;

        foreach (string column in table.KeyColumns)
        {
            ColumnInfo info = table.Column(column);
            string placeholder = CastPlaceholderForPostgres("$" + number++, info);
            where.Add(SqlNames.QuotePg(column) + " = " + placeholder);
            values.Add(Values.ConvertForPostgres(row[column], info, fromBinlog));
        }

        await using var command = new NpgsqlCommand(
            "DELETE FROM " + SqlNames.QuotePg(table.PgTable) +
            " WHERE " + string.Join(" AND ", where), _postgres, _pgTransaction);
        foreach (object? value in values)
            command.Parameters.Add(new NpgsqlParameter { Value = value ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(_forceToken);
    }
}
