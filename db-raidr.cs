using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using MySqlCdc;
using MySqlCdc.Events;
using MySqlCdc.Providers.MySql;
using MySqlConnector;
using Npgsql;
using CdcSslMode = MySqlCdc.Constants.SslMode;

namespace DbRaidr;

/// <summary>
/// MySQL -> PostgreSQL copy using the DBLog watermark algorithm.
///
///     dotnet run -- &lt;workspace_dir&gt;
///
/// The workspace_dir must contain a config.json. It also doubles as the
/// program's scratch space: the state file and anything else the program
/// saves are written inside that same folder.
///
/// Chunked SELECTs from the source table are interleaved with the live binlog
/// stream. Before and after each chunk we insert a marker row ("watermark") into
/// a small MySQL table. Those markers come back through the binlog, which tells us
/// which changes happened while we were reading the chunk. Any snapshot row
/// whose key changed inside that window is dropped, because the change event we
/// already applied is newer. No table locks, no long snapshot transaction.
///
/// The rule that keeps this correct: change events are ALWAYS applied. Watermarks
/// only decide which snapshot rows to throw away.
///
/// Phase 1 backfills every table, then phase 2 follows the binlog until you stop
/// it. Ctrl-C finishes the current unit of work, saves position, and exits.
/// PostgreSQL writes are idempotent (upsert by key, delete by key), so replaying a
/// few events after a crash is harmless.
///
/// Requirements
/// ------------
///     MySqlCdc      - MySQL binlog replication stream
///     MySqlConnector - normal MySQL queries
///     Npgsql        - PostgreSQL client
///
///     binlog_format            = ROW
///     binlog_row_image         = FULL
///     binlog_row_metadata      = FULL     -- MySQL 8.0.14+
///     binlog_row_value_options = ''       -- must not be PARTIAL_JSON
///
///     CREATE TABLE dblog_watermarks (id VARCHAR(64) PRIMARY KEY) ENGINE=InnoDB;
///
/// The watermark table must live in the same schema as the migrated tables, or its
/// rows never reach the stream. Each table's key_columns must be NOT NULL and
/// backed by a unique index on both sides. NOT NULL is checked at startup; the
/// unique index is not, but without it rows get merged and pagination skips rows.
///
/// Passwords are never read from config.json. Each database's password is taken
/// from the environment variable named in that database's password_env field.
/// Column types are read from information_schema, so binary/blob/json/bit/set
/// columns do not need to be declared anywhere.
/// </summary>
internal static class Program
{
    private static volatile bool _stopRequested;
    private static volatile bool _forcedExitRequested;
    private static readonly CancellationTokenSource ForceExit = new();
    private static PosixSignalRegistration? _sigtermRegistration;

    public static async Task<int> Main(string[] args)
    {
        LoadDotEnv();

        if (args.Length != 1)
        {
            Console.Error.WriteLine("usage: db-raidr <workspace_dir>");
            return 1;
        }

        InstallSignalHandlers();

        string workspace = args[0];
        string configPath = Path.Combine(workspace, "config.json");

        AppConfig config;
        await using (var stream = File.OpenRead(configPath))
        {
            config = await JsonSerializer.DeserializeAsync<AppConfig>(stream, JsonOptions())
                     ?? throw new InvalidOperationException("config.json is empty");
        }

        try
        {
            var migration = new Migration(config, workspace, ForceExit.Token);
            await migration.RunAsync();
        }
        catch (OperationCanceledException) when (_forcedExitRequested)
        {
            Log.Error("forced exit - saved state may be behind actual progress");
            return 130;
        }
        catch (InvalidOperationException error)
        {
            Log.Error(error.Message);
            return 2;
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            // Database libraries use several exception types; surface them
            // cleanly as a run error rather than dumping a full stack trace.
            Log.Error(error.Message);
            return 2;
        }

        return 0;
    }

    internal static bool StopRequested => _stopRequested;

    private static void InstallSignalHandlers()
    {
        // First Ctrl-C asks to stop, a second one forces the blocking stream to
        // cancel immediately. The run() finally block still gets a chance to save.
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            RequestStop();
        };

        // Console.CancelKeyPress covers Ctrl-C. Register SIGTERM separately on
        // Unix so service/container shutdown has the same graceful behavior.
        if (!OperatingSystem.IsWindows())
        {
            _sigtermRegistration = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
            {
                context.Cancel = true;
                RequestStop();
            });
        }
    }

    private static void RequestStop()
    {
        if (_stopRequested)
        {
            _forcedExitRequested = true;
            ForceExit.Cancel();
            return;
        }

        _stopRequested = true;
        Log.Warning("interrupt - finishing current chunk, then exiting");
    }

    private static void LoadDotEnv()
    {
        // Find the nearest .env walking upward from the current directory
        // and do not overwrite variables that are already present in the
        // real environment.
        DirectoryInfo? directory = new(Environment.CurrentDirectory);
        while (directory is not null)
        {
            string path = Path.Combine(directory.FullName, ".env");
            if (File.Exists(path))
            {
                foreach (string rawLine in File.ReadLines(path))
                {
                    string line = rawLine.Trim();
                    if (line.Length == 0 || line.StartsWith('#'))
                        continue;
                    if (line.StartsWith("export ", StringComparison.Ordinal))
                        line = line[7..].TrimStart();

                    int equals = line.IndexOf('=');
                    if (equals <= 0)
                        continue;

                    string name = line[..equals].Trim();
                    string value = line[(equals + 1)..].Trim();
                    if ((value.StartsWith('"') && value.EndsWith('"')) ||
                        (value.StartsWith('\'') && value.EndsWith('\'')))
                    {
                        value = value[1..^1];
                    }

                    if (Environment.GetEnvironmentVariable(name) is null)
                        Environment.SetEnvironmentVariable(name, value);
                }
                return;
            }

            directory = directory.Parent;
        }
    }

    internal static string ReadPassword(string envVar)
    {
        // Kept out of config.json so the config is safe to commit. An empty value
        // is accepted; an unset variable is almost always a forgotten export.
        string? password = Environment.GetEnvironmentVariable(envVar);
        if (password is null)
            throw new InvalidOperationException(
                $"Failed reading password - environment variable '{envVar}' is not set");
        return password;
    }

    internal static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = false,
        WriteIndented = true
    };
}

internal sealed class WindowInterruptedException : Exception
{
    // A chunk's watermark window could not be completed.
    public WindowInterruptedException(string message) : base(message) { }
}

internal static class Log
{
    public static void Info(string message, params object?[] args) => Write("INFO", message, args);
    public static void Warning(string message, params object?[] args) => Write("WARNING", message, args);
    public static void Error(string message, params object?[] args) => Write("ERROR", message, args);

    private static void Write(string level, string message, params object?[] args)
    {
        string text = args.Length == 0
            ? message
            : string.Format(CultureInfo.InvariantCulture, message, args);
        Console.Error.WriteLine($"{DateTime.Now:HH:mm:ss} {level,-7} {text}");
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

internal sealed class AppConfig
{
    [JsonPropertyName("mysql")]
    public required DatabaseConfig MySql { get; init; }

    [JsonPropertyName("postgres")]
    public required DatabaseConfig Postgres { get; init; }

    [JsonPropertyName("chunk_size")]
    public int ChunkSize { get; init; } = 1000;

    [JsonPropertyName("watermark_table")]
    public string WatermarkTable { get; init; } = "dblog_watermarks";

    [JsonPropertyName("watermark_timeout_seconds")]
    public double WatermarkTimeoutSeconds { get; init; } = 120;

    [JsonPropertyName("state_file")]
    public string StateFile { get; init; } = "dblog_state.json";

    [JsonPropertyName("server_id")]
    public long ServerId { get; init; } = 100;

    [JsonPropertyName("heartbeat_seconds")]
    public double HeartbeatSeconds { get; init; } = 10;

    [JsonPropertyName("tables")]
    public required List<TableConfig> Tables { get; init; }
}

internal sealed class DatabaseConfig
{
    [JsonPropertyName("host")]
    public string Host { get; init; } = "127.0.0.1";

    [JsonPropertyName("port")]
    public int Port { get; init; }

    [JsonPropertyName("user")]
    public required string User { get; init; }

    [JsonPropertyName("db")]
    public required string Db { get; init; }

    [JsonPropertyName("password_env")]
    public required string PasswordEnv { get; init; }
}

internal sealed class TableConfig
{
    [JsonPropertyName("mysql_table")]
    public required string MySqlTable { get; init; }

    [JsonPropertyName("pg_table")]
    public required string PgTable { get; init; }

    [JsonPropertyName("key_columns")]
    public required List<string> KeyColumns { get; init; }

    [JsonPropertyName("skip_columns")]
    public List<string> SkipColumns { get; init; } = [];
}

// ---------------------------------------------------------------------------
// Values
//
// The same row reaches us twice by two different routes: once from a SELECT
// (MySqlConnector) and once from the binlog (MySqlCdc). The two libraries do
// not always return the same CLR object for the same column, so every value is
// put into one canonical form based on what information_schema says the column is.
// ---------------------------------------------------------------------------

internal sealed class ColumnInfo
{
    public required string Name { get; init; }
    public required string Kind { get; init; }
    public required string SourceType { get; init; }
    public bool Unsigned { get; init; }
    public IReadOnlyList<string> TypeMembers { get; init; } = [];
}

internal static class Values
{
    public static object? ToText(object? value)
    {
        // Decode bytes to string; anything else passes through unchanged.
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            Memory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            _ => value
        };
    }

    public static byte[] ToBytes(object value)
    {
        // Encode a string to bytes; byte-like values pass through unchanged.
        return value switch
        {
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            Memory<byte> memory => memory.ToArray(),
            string text => Encoding.UTF8.GetBytes(text),
            _ => throw new InvalidOperationException(
                $"cannot coerce {value.GetType().Name} to bytes without losing information")
        };
    }

    public static object BitToInteger(object value)
    {
        // A SELECT and the binlog represent BIT differently. Convert both to one
        // integer representation so key identity and PostgreSQL writes agree.
        if (value is bool boolean)
            return boolean ? 1L : 0L;

        if (value is bool[] bits)
        {
            BigInteger result = BigInteger.Zero;
            foreach (bool bit in bits)
                result = (result << 1) | (bit ? BigInteger.One : BigInteger.Zero);
            return NarrowBigInteger(result);
        }

        if (value is BitArray bitArray)
        {
            BigInteger result = BigInteger.Zero;
            for (int i = 0; i < bitArray.Length; i++)
                result = (result << 1) | (bitArray[i] ? BigInteger.One : BigInteger.Zero);
            return NarrowBigInteger(result);
        }

        if (value is byte[] bytes)
        {
            BigInteger result = BigInteger.Zero;
            foreach (byte item in bytes)
                result = (result << 8) | item;
            return NarrowBigInteger(result);
        }

        return value;
    }

    private static object NarrowBigInteger(BigInteger value)
    {
        if (value >= long.MinValue && value <= long.MaxValue)
            return (long)value;
        return value;
    }

    public static object? CanonicalSourceValue(object? value, ColumnInfo info, bool fromBinlog)
    {
        if (value is null || value is DBNull)
            return null;

        // MySqlCdc needs help with a few types whose raw representation differs
        // from MySqlConnector. Normalize those differences before key comparison.
        value = NormalizeSignedness(value, info, fromBinlog);

        if (info.Kind == "binary")
            return ToBytes(value);
        if (info.Kind == "bit")
            return BitToInteger(value);
        if (info.Kind == "json")
            return JsonToText(value, fromBinlog);
        if (info.Kind == "set")
            return SetToText(value, info.TypeMembers);
        if (info.Kind == "enum")
            return EnumToText(value, info.TypeMembers);
        if (info.Kind == "decimal")
            return DecimalToText(value);

        if (info.SourceType == "date")
        {
            if (value is DateTime dateTime)
                return DateOnly.FromDateTime(dateTime);
            return value;
        }

        // MySqlCdc exposes TIMESTAMP as DateTimeOffset while normal SELECTs are
        // DateTime. Put both on a timezone-free DateTime so the two routes agree.
        if (info.SourceType == "timestamp" && value is DateTimeOffset dto)
            return DateTime.SpecifyKind(dto.UtcDateTime, DateTimeKind.Unspecified);

        return value;
    }

    public static object? ConvertForPostgres(object? value, ColumnInfo info, bool fromBinlog)
    {
        // One MySQL value -> something Npgsql can send.
        value = CanonicalSourceValue(value, info, fromBinlog);
        if (value is null)
            return null;

        if (info.Kind is "binary" or "bit" or "json" or "set" or "enum" or "decimal")
            return value;

        if (value is byte[] bytes)
            return Encoding.UTF8.GetString(bytes);

        if (value is TimeSpan duration)
            return FormatMySqlTime(duration); // MySQL TIME

        if (value is MySqlDecimal mysqlDecimal)
            return mysqlDecimal.ToString();

        return value;
    }

    private static object NormalizeSignedness(object value, ColumnInfo info, bool fromBinlog)
    {
        if (!fromBinlog)
            return value;

        // MySqlCdc documents integer storage in fixed CLR types. Reinterpret the
        // raw bits for UNSIGNED columns when necessary, and keep signed tinyints
        // signed so SELECT and binlog values compare the same.
        return info.SourceType switch
        {
            "tinyint" when info.Unsigned && value is sbyte v => unchecked((byte)v),
            "tinyint" when !info.Unsigned && value is byte v => unchecked((sbyte)v),
            "smallint" when info.Unsigned && value is short v => unchecked((ushort)v),
            "mediumint" when info.Unsigned && value is int v => ((uint)v << 8) >> 8,
            "int" when info.Unsigned && value is int v => unchecked((uint)v),
            "bigint" when info.Unsigned && value is long v => unchecked((ulong)v),
            _ => value
        };
    }

    private static string JsonToText(object value, bool fromBinlog)
    {
        // A SELECT gives JSON already serialized. MySqlCdc gives MySQL JSON as
        // its binary on-wire representation, so parse it back to valid JSON text.
        if (!fromBinlog)
            return Convert.ToString(ToText(value), CultureInfo.InvariantCulture) ?? "null";

        if (value is byte[] bytes)
            return JsonParser.Parse(bytes);
        if (value is ReadOnlyMemory<byte> memory)
            return JsonParser.Parse(memory.ToArray());
        if (value is string text)
            return text;

        return JsonSerializer.Serialize(value);
    }

    private static string SetToText(object value, IReadOnlyList<string> members)
    {
        // MySQL SET: SELECT gives "a,b"; MySqlCdc gives the numeric bit mask.
        // Sorting both routes is the point: the row does not change value based
        // on which route it arrived through.
        IEnumerable<string> values;
        if (value is string text)
        {
            values = text.Length == 0 ? [] : text.Split(',');
        }
        else if (value is byte[] bytes)
        {
            string textValue = Encoding.UTF8.GetString(bytes);
            values = textValue.Length == 0 ? [] : textValue.Split(',');
        }
        else if (TryUnsignedInteger(value, out ulong mask))
        {
            var selected = new List<string>();
            for (int i = 0; i < members.Count && i < 64; i++)
            {
                if ((mask & (1UL << i)) != 0)
                    selected.Add(members[i]);
            }
            values = selected;
        }
        else if (value is IEnumerable enumerable)
        {
            var selected = new List<string>();
            foreach (object? item in enumerable)
                selected.Add(Convert.ToString(ToText(item), CultureInfo.InvariantCulture) ?? string.Empty);
            values = selected;
        }
        else
        {
            values = [Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty];
        }

        return string.Join(',', values.OrderBy(value => value, StringComparer.Ordinal));
    }

    private static string EnumToText(object value, IReadOnlyList<string> members)
    {
        // MySqlConnector SELECTs return the label; MySqlCdc represents ENUM by
        // its 1-based numeric index. Convert the binlog form back to the label.
        if (value is string text)
            return text;
        if (value is byte[] bytes)
            return Encoding.UTF8.GetString(bytes);

        if (TryUnsignedInteger(value, out ulong index))
        {
            if (index == 0)
                return string.Empty;
            if (index <= (ulong)members.Count)
                return members[(int)index - 1];
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static string DecimalToText(object value)
    {
        return value switch
        {
            MySqlDecimal mysqlDecimal => mysqlDecimal.ToString(),
            decimal number => number.ToString(CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }

    private static bool TryUnsignedInteger(object value, out ulong result)
    {
        switch (value)
        {
            case byte v: result = v; return true;
            case sbyte v: result = unchecked((ulong)v); return true;
            case ushort v: result = v; return true;
            case short v: result = unchecked((ulong)v); return true;
            case uint v: result = v; return true;
            case int v: result = unchecked((ulong)v); return true;
            case ulong v: result = v; return true;
            case long v: result = unchecked((ulong)v); return true;
            default:
                result = 0;
                return false;
        }
    }

    private static string FormatMySqlTime(TimeSpan value)
    {
        // Format MySQL TIME as days, hours, minutes, seconds, and microseconds,
        // including negative-value normalization (days go more negative while
        // the time-of-day remainder stays non-negative).
        const long microsPerSecond = 1_000_000;
        const long microsPerDay = 86_400 * microsPerSecond;

        long totalMicros = value.Ticks / 10;
        long days = FloorDiv(totalMicros, microsPerDay);
        long remainder = totalMicros - days * microsPerDay;

        long hours = remainder / (3_600 * microsPerSecond);
        remainder %= 3_600 * microsPerSecond;
        long minutes = remainder / (60 * microsPerSecond);
        remainder %= 60 * microsPerSecond;
        long seconds = remainder / microsPerSecond;
        long micros = remainder % microsPerSecond;

        string time = micros == 0
            ? $"{hours}:{minutes:00}:{seconds:00}"
            : $"{hours}:{minutes:00}:{seconds:00}.{micros:000000}";

        if (days == 0)
            return time;
        return $"{days} day{(Math.Abs(days) == 1 ? string.Empty : "s")}, {time}";
    }

    private static long FloorDiv(long value, long divisor)
    {
        long quotient = value / divisor;
        long remainder = value % divisor;
        if (remainder != 0 && value < 0)
            quotient--;
        return quotient;
    }
}

// ---------------------------------------------------------------------------
// Keys
//
// This has to be exactly right. If the snapshot copy and the binlog copy of one
// row do not produce the same key, the stale snapshot row is not discarded and
// it overwrites the newer change event. That is silent data loss, and it only
// affects rows written during the backfill.
//
// Binary keys are the dangerous case: random bytes are usually not valid UTF-8,
// so decoding them loses information. We hex them instead, which is reversible.
// The type name is part of the identity, so 5 and "5" never compare equal.
// ---------------------------------------------------------------------------

internal readonly record struct EncodedValue(string Type, string Text);

internal sealed class RowKey : IEquatable<RowKey>
{
    public EncodedValue[] Parts { get; }

    public RowKey(IEnumerable<EncodedValue> parts)
    {
        Parts = parts.ToArray();
    }

    public bool Equals(RowKey? other)
    {
        if (ReferenceEquals(this, other))
            return true;
        if (other is null || Parts.Length != other.Parts.Length)
            return false;
        for (int i = 0; i < Parts.Length; i++)
        {
            if (Parts[i] != other.Parts[i])
                return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is RowKey key && Equals(key);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (EncodedValue part in Parts)
            hash.Add(part);
        return hash.ToHashCode();
    }
}

internal static class Keys
{
    public static object? NormalizeKey(object? value, ColumnInfo info, bool fromBinlog)
    {
        // Put a raw key value into the canonical form used for identity
        // comparisons before it is handed to EncodeValue().
        value = Values.CanonicalSourceValue(value, info, fromBinlog);
        if (value is null)
            return null;
        if (info.Kind == "binary")
            return Values.ToBytes(value);
        if (info.Kind == "bit")
            return Values.BitToInteger(value);
        return Values.ToText(value);
    }

    public static EncodedValue EncodeValue(object? value)
    {
        // Turn a normalized key value into a (type, text) pair that is both
        // JSON-serializable and hashable/comparable, and DecodeValue can rebuild.
        if (value is null)
            return new("null", string.Empty);
        if (value is byte[] bytes)
            return new("bytes", Convert.ToHexString(bytes).ToLowerInvariant());
        if (value is bool boolean)
            return new("bool", boolean ? "1" : "0");
        if (value is sbyte or byte or short or ushort or int or uint or long or ulong or BigInteger)
            return new("int", Convert.ToString(value, CultureInfo.InvariantCulture) ?? "0");
        if (value is decimal number)
            return new("decimal", number.ToString(CultureInfo.InvariantCulture));
        if (value is MySqlDecimal mysqlDecimal)
            return new("decimal", mysqlDecimal.ToString());
        if (value is DateTime dateTime)
            return new("datetime", dateTime.ToString("O", CultureInfo.InvariantCulture));
        if (value is DateOnly date)
            return new("date", date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        if (value is TimeOnly time)
            return new("time", time.ToString("O", CultureInfo.InvariantCulture));
        if (value is float single)
            return new("float", single.ToString("R", CultureInfo.InvariantCulture));
        if (value is double floating)
            return new("float", floating.ToString("R", CultureInfo.InvariantCulture));
        return new("str", Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
    }

    public static object? DecodeValue(EncodedValue pair)
    {
        // Reverse of EncodeValue(), used when resuming a backfill from state.
        return pair.Type switch
        {
            "null" => null,
            "bytes" => Convert.FromHexString(pair.Text),
            "bool" => pair.Text == "1",
            "int" => DecodeInteger(pair.Text),
            "decimal" => decimal.TryParse(pair.Text, NumberStyles.Number,
                CultureInfo.InvariantCulture, out decimal d) ? d : pair.Text,
            "datetime" => DateTime.Parse(pair.Text, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind),
            "date" => DateOnly.Parse(pair.Text, CultureInfo.InvariantCulture),
            "time" => TimeOnly.Parse(pair.Text, CultureInfo.InvariantCulture),
            "float" => double.Parse(pair.Text, CultureInfo.InvariantCulture),
            _ => pair.Text
        };
    }

    private static object DecodeInteger(string text)
    {
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long signed))
            return signed;
        if (ulong.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong unsigned))
            return unsigned;
        return BigInteger.Parse(text, CultureInfo.InvariantCulture);
    }

    public static RowKey RowIdentity(Dictionary<string, object?> row, Table table, bool fromBinlog)
    {
        // The comparable identity of a row, whichever route it came by.
        var parts = new List<EncodedValue>(table.KeyColumns.Count);
        foreach (string column in table.KeyColumns)
        {
            object? value = row[column];
            ColumnInfo info = table.Column(column);
            parts.Add(EncodeValue(NormalizeKey(value, info, fromBinlog)));
        }
        return new RowKey(parts);
    }
}

internal static class SqlNames
{
    public static string QuotePg(string name)
    {
        // Double-quote a PostgreSQL identifier, escaping embedded quotes. Handles
        // "schema.table" by quoting each dot-separated part on its own.
        return string.Join('.', name.Split('.').Select(part =>
            "\"" + part.Replace("\"", "\"\"") + "\""));
    }

    public static string QuoteMySql(string name)
    {
        // Backtick-quote a MySQL identifier, escaping embedded backticks.
        return "`" + name.Replace("`", "``") + "`";
    }
}

internal sealed class Table
{
    // One table's migration config, plus column kinds discovered later from
    // information_schema (see Migration.LoadKindsAsync).
    public string MySqlTable { get; }
    public string PgTable { get; }
    public IReadOnlyList<string> KeyColumns { get; }
    public HashSet<string> SkipColumns { get; }
    public List<string> Columns { get; } = [];
    private readonly Dictionary<string, ColumnInfo> _columnInfo = new(StringComparer.Ordinal);

    public Table(TableConfig raw)
    {
        // raw is one entry of the "tables" list from config.json.
        MySqlTable = raw.MySqlTable;
        PgTable = raw.PgTable;
        KeyColumns = raw.KeyColumns;
        SkipColumns = new HashSet<string>(raw.SkipColumns, StringComparer.Ordinal);

        if (KeyColumns.Count == 0)
            throw new InvalidOperationException(MySqlTable + ": key_columns is empty");
        if (KeyColumns.Any(SkipColumns.Contains))
            throw new InvalidOperationException(MySqlTable + ": a key column is skipped");
    }

    public void AddColumn(ColumnInfo info)
    {
        Columns.Add(info.Name);
        _columnInfo[info.Name] = info;
    }

    public ColumnInfo Column(string name)
    {
        if (!_columnInfo.TryGetValue(name, out ColumnInfo? info))
            throw new InvalidOperationException(MySqlTable + ": no column " + name);
        return info;
    }

    public string Kind(string column) => Column(column).Kind;
}

internal sealed class TableProgress
{
    public bool Done { get; set; }
    public List<EncodedValue>? LastKey { get; set; }
    public List<string> KeyColumns { get; set; } = [];
}

internal sealed class State
{
    // Binlog position plus per-table progress, written atomically.
    public string Path { get; }
    public string? LogFile { get; set; }
    public long? LogPos { get; set; }
    public Dictionary<string, TableProgress> Tables { get; } = new(StringComparer.Ordinal);

    public State(string path)
    {
        Path = path;

        // If a state file already exists, load it so a rerun resumes instead of
        // starting the backfill and binlog stream over from scratch.
        if (!File.Exists(path))
            return;

        using JsonDocument document = JsonDocument.Parse(File.ReadAllText(path));
        JsonElement root = document.RootElement;
        if (root.TryGetProperty("log_file", out JsonElement logFile) && logFile.ValueKind != JsonValueKind.Null)
            LogFile = logFile.GetString();
        if (root.TryGetProperty("log_pos", out JsonElement logPos) && logPos.ValueKind != JsonValueKind.Null)
            LogPos = logPos.GetInt64();

        if (root.TryGetProperty("tables", out JsonElement tables))
        {
            foreach (JsonProperty property in tables.EnumerateObject())
            {
                JsonElement entry = property.Value;
                var progress = new TableProgress
                {
                    Done = entry.GetProperty("done").GetBoolean(),
                    KeyColumns = entry.GetProperty("key_columns")
                        .EnumerateArray().Select(item => item.GetString() ?? string.Empty).ToList()
                };

                JsonElement lastKey = entry.GetProperty("last_key");
                if (lastKey.ValueKind != JsonValueKind.Null)
                {
                    progress.LastKey = [];
                    foreach (JsonElement pair in lastKey.EnumerateArray())
                    {
                        string type = pair[0].GetString() ?? string.Empty;
                        string text = pair[1].GetString() ?? string.Empty;
                        progress.LastKey.Add(new EncodedValue(type, text));
                    }
                }

                Tables[property.Name] = progress;
            }
        }

        Log.Info("resuming at {0}:{1}", LogFile ?? "", LogPos?.ToString(CultureInfo.InvariantCulture) ?? "");
    }

    public void Save()
    {
        // Write state to a temp file and rename over the real path, so a crash
        // mid-write never leaves a half-written, unreadable state file.
        string temp = Path + ".tmp";
        var tableObjects = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach ((string name, TableProgress entry) in Tables)
        {
            tableObjects[name] = new Dictionary<string, object?>
            {
                ["done"] = entry.Done,
                ["last_key"] = entry.LastKey?.Select(pair => new[] { pair.Type, pair.Text }).ToList(),
                ["key_columns"] = entry.KeyColumns
            };
        }

        var root = new Dictionary<string, object?>
        {
            ["log_file"] = LogFile,
            ["log_pos"] = LogPos,
            ["tables"] = tableObjects
        };

        byte[] json = JsonSerializer.SerializeToUtf8Bytes(root, Program.JsonOptions());
        using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(json);
            stream.Flush(flushToDisk: true);
        }
        File.Move(temp, Path, overwrite: true);
    }

    public TableProgress Progress(Table table)
    {
        // Get (or create) this table's progress entry. Refuse to reuse a saved
        // entry whose key_columns no longer match the config.
        if (!Tables.TryGetValue(table.MySqlTable, out TableProgress? entry))
        {
            entry = new TableProgress
            {
                Done = false,
                LastKey = null,
                KeyColumns = table.KeyColumns.ToList()
            };
            Tables[table.MySqlTable] = entry;
        }
        else if (!entry.KeyColumns.SequenceEqual(table.KeyColumns, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(table.MySqlTable +
                ": key_columns changed since the saved run - delete the state file");
        }

        return entry;
    }
}

// ---------------------------------------------------------------------------

internal sealed class Migration
{
    // Owns both database connections plus all migration state, and drives the
    // two phases: BackfillAsync() per table, then a live ApplyEventAsync() loop.
    private static readonly HashSet<string> DdlWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"
    };

    private static readonly HashSet<string> BinaryTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "geometry"
    };

    private readonly AppConfig _config;
    private readonly string _workspace;
    private readonly int _chunkSize;
    private readonly string _watermarkTable;
    private readonly TimeSpan _timeout;
    private readonly string _db;
    private readonly CancellationToken _forceToken;

    private readonly string _mysqlPassword;
    private readonly MySqlConnection _mysql;
    private readonly NpgsqlConnection _postgres;
    private NpgsqlTransaction _pgTransaction;

    private readonly List<Table> _tables;
    private readonly Dictionary<string, Table> _byName;
    private readonly State _state;
    private readonly Dictionary<long, TableMapEvent> _tableMaps = new();

    private BinlogClient? _stream;
    private string? _currentLogFile;
    private long? _currentLogPos;

    public Migration(AppConfig config, string workspace, CancellationToken forceToken)
    {
        // Open both connections immediately, so a bad config, missing password
        // variable or unreachable database fails at startup instead of mid-run.
        _config = config;
        _workspace = workspace;
        _chunkSize = config.ChunkSize;
        _watermarkTable = config.WatermarkTable;
        _timeout = TimeSpan.FromSeconds(config.WatermarkTimeoutSeconds);
        _db = config.MySql.Db;
        _forceToken = forceToken;

        _mysqlPassword = Program.ReadPassword(config.MySql.PasswordEnv);
        string pgPassword = Program.ReadPassword(config.Postgres.PasswordEnv);

        var mysqlBuilder = new MySqlConnectionStringBuilder
        {
            Server = config.MySql.Host,
            Port = (uint)(config.MySql.Port == 0 ? 3306 : config.MySql.Port),
            UserID = config.MySql.User,
            Password = _mysqlPassword,
            Database = _db,
            CharacterSet = "utf8mb4",
            TreatTinyAsBoolean = false,
            GuidFormat = MySqlGuidFormat.None,
            DateTimeKind = MySqlDateTimeKind.Unspecified
        };
        _mysql = new MySqlConnection(mysqlBuilder.ConnectionString);
        _mysql.Open();

        var pgBuilder = new NpgsqlConnectionStringBuilder
        {
            Host = config.Postgres.Host,
            Port = config.Postgres.Port == 0 ? 5432 : config.Postgres.Port,
            Username = config.Postgres.User,
            Password = pgPassword,
            Database = config.Postgres.Db
        };
        _postgres = new NpgsqlConnection(pgBuilder.ConnectionString);
        _postgres.Open();
        _pgTransaction = _postgres.BeginTransaction();

        _tables = config.Tables.Select(raw => new Table(raw)).ToList();
        _byName = _tables.ToDictionary(table => table.MySqlTable, StringComparer.Ordinal);
        _state = new State(System.IO.Path.Combine(_workspace, config.StateFile));
    }

    // -- setup --------------------------------------------------------------

    private async Task CheckSettingsAsync()
    {
        // Bad settings fail late and confusingly: MINIMAL row images write rows
        // full of missing values, and PARTIAL_JSON logs JSON diffs that wipe out
        // the rest of the document when replayed.
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

    private async Task LoadKindsAsync(Table table)
    {
        // Ask MySQL what each column really is. Trusting a hand-written config
        // means one missed BLOB gets decoded as UTF-8 and stored as mojibake.
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

    private static IReadOnlyList<string> ParseTypeMembers(string columnType)
    {
        // Parse information_schema COLUMN_TYPE such as:
        //     set('a','b') / enum('one','two')
        // including ordinary MySQL backslash escapes and doubled quotes.
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

    private async Task OpenStreamAsync()
    {
        // blocking=True equivalent is required: a non-blocking reader stops as
        // soon as it drains, so the loop waiting for a high watermark exits early.
        // Heartbeats keep the iterator waking up on an idle server.
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

    private async IAsyncEnumerable<IBinlogEvent> StreamEventsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_stream is null)
            throw new InvalidOperationException("binlog stream is not open");

        await foreach ((EventHeader header, IBinlogEvent binlogEvent) in
                       _stream.Replicate(cancellationToken).WithCancellation(cancellationToken))
        {
            // MySqlCdc updates client.State only after control returns from the
            // yielded event. Track the position ourselves first so a checkpoint
            // taken while applying XidEvent records the next-event position.
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

    // -- writing ------------------------------------------------------------

    private async Task CheckpointAsync()
    {
        // Commit PostgreSQL first, record the position second, so the saved
        // position can only lag the data. Lagging just replays idempotent writes.
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

    private async Task UpsertAsync(Table table, List<Dictionary<string, object?>> rows)
    {
        // Bulk INSERT ... ON CONFLICT DO UPDATE the given rows into PostgreSQL.
        // Idempotent by design, so replaying after crash-and-resume is harmless.
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

    private static string CastPlaceholderForPostgres(string placeholder, ColumnInfo info)
    {
        // Npgsql sends strings as PostgreSQL text. Explicit casts send
        // arbitrary-precision DECIMAL and JSON text to the intended types.
        return info.Kind switch
        {
            "decimal" => placeholder + "::numeric",
            "json" => placeholder + "::jsonb",
            _ => placeholder
        };
    }

    private async Task DeleteAsync(Table table, Dictionary<string, object?> row, bool fromBinlog)
    {
        // DELETE the row matching this table's key columns. Deleting a key that is
        // already gone is a no-op, so this is safe to replay too.
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

    // -- events -------------------------------------------------------------

    private List<string> MarksIn(IBinlogEvent binlogEvent)
    {
        // The watermark ids inserted by this event, if it is a write to the
        // watermark table; otherwise an empty list. follow_to() uses this to spot
        // the low/high markers as they come back through the binlog stream.
        if (binlogEvent is not WriteRowsEvent writeRows)
            return [];
        if (!_tableMaps.TryGetValue(writeRows.TableId, out TableMapEvent? map))
            return [];
        if (!string.Equals(map.DatabaseName, _db, StringComparison.Ordinal) ||
            !string.Equals(map.TableName, _watermarkTable, StringComparison.Ordinal))
            return [];

        int idIndex = 0;
        IReadOnlyList<string>? names = map.TableMetadata?.ColumnNames;
        if (names is not null)
        {
            for (int i = 0; i < names.Count; i++)
            {
                if (string.Equals(names[i], "id", StringComparison.Ordinal))
                {
                    idIndex = i;
                    break;
                }
            }
        }

        var marks = new List<string>();
        foreach (RowData row in writeRows.Rows)
        {
            if (idIndex >= row.Cells.Count)
                continue;
            object? value = Values.ToText(row.Cells[idIndex]);
            if (value is not null)
                marks.Add(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
        }
        return marks;
    }

    private async Task ApplyEventAsync(IBinlogEvent binlogEvent)
    {
        // Change events are applied unconditionally, window open or not.
        // Rows go one at a time, in binlog order. Batching would reorder
        // operations on the same key and produce the wrong final state.
        if (binlogEvent is XidEvent)
        {
            await CheckpointAsync(); // a source commit boundary
            return;
        }

        if (binlogEvent is QueryEvent queryEvent)
        {
            string query = (queryEvent.SqlStatement ?? string.Empty).Trim();
            if (query.Length > 0)
            {
                string first = query.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)[0];
                if (DdlWords.Contains(first) && QueryTouchesTargetSchema(queryEvent, query))
                {
                    throw new InvalidOperationException(
                        "source DDL cannot be replayed: " + query[..Math.Min(200, query.Length)]);
                }
            }
            return;
        }

        if (binlogEvent is TableMapEvent or HeartbeatEvent or RotateEvent)
            return;

        // MySqlCdc has no shared rows-event interface; treat Write, Update,
        // and Delete events as the row-change types.
        if (!TryGetRowsEventTableId(binlogEvent, out long tableId))
            return;

        if (!_tableMaps.TryGetValue(tableId, out TableMapEvent? map))
            throw new InvalidOperationException("row event arrived without its TableMapEvent");

        if (!string.Equals(map.DatabaseName, _db, StringComparison.Ordinal))
            return;

        if (!_byName.TryGetValue(map.TableName, out Table? table))
            return; // watermark rows and unconfigured tables are signals/ignored

        foreach ((Dictionary<string, object?>? before, Dictionary<string, object?>? after) in
                 RowChanges(binlogEvent, table))
        {
            if (after is null)
            {
                await DeleteAsync(table, before!, fromBinlog: true);
                continue;
            }

            // An update that changes a key moves the row; without the delete the
            // old copy is left behind in PostgreSQL forever.
            if (before is not null &&
                !Keys.RowIdentity(before, table, true).Equals(Keys.RowIdentity(after, table, true)))
            {
                await DeleteAsync(table, before, fromBinlog: true);
            }

            await UpsertAsync(table, [ConvertRow(after, table, fromBinlog: true)]);
        }
    }

    private bool QueryTouchesTargetSchema(QueryEvent queryEvent, string query)
    {
        if (string.Equals(queryEvent.DatabaseName, _db, StringComparison.Ordinal))
            return true;

        // MySqlCdc streams all server schemas. Avoid aborting on unrelated
        // DDL, but still catch explicit schema-qualified DDL.
        return query.Contains("`" + _db.Replace("`", "``") + "`.", StringComparison.OrdinalIgnoreCase)
               || query.Contains(_db + ".", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetRowsEventTableId(IBinlogEvent binlogEvent, out long tableId)
    {
        // Write/Update/Delete row events each expose TableId, but they do not
        // share a library base type. Pattern-match the three concrete types.
        switch (binlogEvent)
        {
            case WriteRowsEvent write:
                tableId = write.TableId;
                return true;
            case UpdateRowsEvent update:
                tableId = update.TableId;
                return true;
            case DeleteRowsEvent delete:
                tableId = delete.TableId;
                return true;
            default:
                tableId = 0;
                return false;
        }
    }

    private IEnumerable<(Dictionary<string, object?>? Before, Dictionary<string, object?>? After)>
        RowChanges(IBinlogEvent binlogEvent, Table table)
    {
        // (before, after) pairs. Insert has no before, delete has no after.
        switch (binlogEvent)
        {
            case UpdateRowsEvent update:
                foreach (UpdateRowData row in update.Rows)
                {
                    yield return (BuildRow(row.BeforeUpdate, table), BuildRow(row.AfterUpdate, table));
                }
                yield break;

            case DeleteRowsEvent delete:
                foreach (RowData row in delete.Rows)
                    yield return (BuildRow(row, table), null);
                yield break;

            case WriteRowsEvent write:
                foreach (RowData row in write.Rows)
                    yield return (null, BuildRow(row, table));
                yield break;
        }
    }

    private static Dictionary<string, object?> BuildRow(RowData row, Table table)
    {
        if (row.Cells.Count != table.Columns.Count)
        {
            throw new InvalidOperationException(
                $"{table.MySqlTable}: binlog row has {row.Cells.Count} cells but information_schema has " +
                $"{table.Columns.Count} columns; FULL row metadata/image is required");
        }

        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (int i = 0; i < table.Columns.Count; i++)
            result[table.Columns[i]] = row.Cells[i];
        return result;
    }

    private static Dictionary<string, object?> ConvertRow(
        Dictionary<string, object?> row, Table table, bool fromBinlog)
    {
        // Apply conversion to every column of a row, dropping skipped columns.
        var converted = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach ((string column, object? value) in row)
        {
            if (!table.SkipColumns.Contains(column))
            {
                converted[column] = Values.ConvertForPostgres(
                    value, table.Column(column), fromBinlog);
            }
        }
        return converted;
    }

    // -- backfill -----------------------------------------------------------

    private async Task<string> WatermarkAsync()
    {
        // Insert a fresh, uniquely-identifiable marker row into the watermark
        // table and return its id, so it can be recognized later in the stream.
        string mark = Guid.NewGuid().ToString("N");
        await using var command = _mysql.CreateCommand();
        command.CommandText = "INSERT INTO " + SqlNames.QuoteMySql(_watermarkTable) +
                              " (id) VALUES (@id)";
        command.Parameters.AddWithValue("@id", mark);
        await command.ExecuteNonQueryAsync(_forceToken);
        return mark;
    }

    private async Task<List<Dictionary<string, object?>>> FetchChunkAsync(
        Table table, List<object?>? afterKey)
    {
        // Row-constructor comparison, so composite keys of any comparable type
        // work and we never assume keys are numeric or contiguous.
        string keys = string.Join(", ", table.KeyColumns.Select(SqlNames.QuoteMySql));
        string order = string.Join(", ", table.KeyColumns.Select(column =>
            SqlNames.QuoteMySql(column) + " ASC"));
        string name = SqlNames.QuoteMySql(table.MySqlTable);

        await using var command = _mysql.CreateCommand();
        if (afterKey is null)
        {
            command.CommandText = "SELECT * FROM " + name + " ORDER BY " + order + " LIMIT @limit";
        }
        else
        {
            var holes = new List<string>();
            for (int i = 0; i < table.KeyColumns.Count; i++)
            {
                string parameter = "@k" + i;
                holes.Add(parameter);
                command.Parameters.AddWithValue(parameter, afterKey[i] ?? DBNull.Value);
            }
            command.CommandText = "SELECT * FROM " + name + " WHERE (" + keys + ") > (" +
                                  string.Join(", ", holes) + ") ORDER BY " + order + " LIMIT @limit";
        }
        command.Parameters.AddWithValue("@limit", _chunkSize);

        var rows = new List<Dictionary<string, object?>>();
        await using var reader = await command.ExecuteReaderAsync(_forceToken);
        while (await reader.ReadAsync(_forceToken))
        {
            var row = new Dictionary<string, object?>(StringComparer.Ordinal);
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    private async Task BackfillAsync(Table table)
    {
        // Copy one table from MySQL to PostgreSQL, chunk by chunk, using the DBLog
        // watermark algorithm to stay correct against concurrent writes. Resumes
        // from entry.LastKey if a previous run left off partway, and returns
        // without marking the table done if interrupted.
        TableProgress entry = _state.Progress(table);
        if (entry.Done)
            return;

        List<object?>? afterKey = null;
        if (entry.LastKey is { Count: > 0 })
            afterKey = entry.LastKey.Select(Keys.DecodeValue).ToList();

        Log.Info("{0}: backfilling", table.MySqlTable);
        int copied = 0;

        while (!Program.StopRequested)
        {
            // Keeps the marker table at two rows instead of two per chunk.
            await using (var clear = _mysql.CreateCommand())
            {
                clear.CommandText = "DELETE FROM " + SqlNames.QuoteMySql(_watermarkTable);
                await clear.ExecuteNonQueryAsync(_forceToken);
            }

            string low = await WatermarkAsync();
            List<Dictionary<string, object?>> chunk = await FetchChunkAsync(table, afterKey); // no lock held
            if (chunk.Count == 0)
            {
                entry.Done = true;
                await CheckpointAsync();
                Log.Info("{0}: done ({1} rows this run)", table.MySqlTable, copied);
                return;
            }

            var byKey = new Dictionary<RowKey, Dictionary<string, object?>>();
            foreach (Dictionary<string, object?> row in chunk)
                byKey[Keys.RowIdentity(row, table, fromBinlog: false)] = row;

            List<object?> nextKey = table.KeyColumns.Select(column => chunk[^1][column]).ToList();
            string high = await WatermarkAsync();

            HashSet<RowKey> changed;
            try
            {
                changed = await FollowToAsync(table, low, high);
            }
            catch (WindowInterruptedException reason)
            {
                // Without the high watermark we cannot tell which snapshot rows
                // are stale, so the chunk is dropped and re-read next run.
                // Nothing is lost: the change events were applied already.
                Log.Warning("{0}: {1} - redoing this chunk", table.MySqlTable, reason.Message);
                await CheckpointAsync();
                return;
            }

            // Keep only rows not touched inside the window. For the rest, the
            // change event we already applied is newer than the SELECT.
            var fresh = new List<Dictionary<string, object?>>();
            foreach ((RowKey key, Dictionary<string, object?> row) in byKey)
            {
                if (!changed.Contains(key))
                    fresh.Add(ConvertRow(row, table, fromBinlog: false));
            }
            await UpsertAsync(table, fresh);

            afterKey = nextKey;
            entry.LastKey = [];
            for (int i = 0; i < nextKey.Count; i++)
            {
                string column = table.KeyColumns[i];
                object? canonical = Values.CanonicalSourceValue(nextKey[i], table.Column(column), false);
                entry.LastKey.Add(Keys.EncodeValue(canonical));
            }

            await CheckpointAsync();
            copied += chunk.Count;
            Log.Info("{0}: +{1} rows ({2} superseded), {3} total",
                table.MySqlTable, fresh.Count, byKey.Count - fresh.Count, copied);
        }
    }

    private async Task<HashSet<RowKey>> FollowToAsync(Table table, string low, string high)
    {
        // Apply events until the high watermark shows up, returning the keys of
        // this table's rows that changed between the two watermarks.
        //
        // The timeout measures silence, not elapsed time: a wall-clock deadline
        // would starve a busy table, discarding and re-reading the same chunk forever.
        bool inside = false;
        var changed = new HashSet<RowKey>();
        long deadline = Stopwatch.GetTimestamp() + (long)(_timeout.TotalSeconds * Stopwatch.Frequency);

        await foreach (IBinlogEvent binlogEvent in StreamEventsAsync(_forceToken))
        {
            if (binlogEvent is not HeartbeatEvent)
                deadline = Stopwatch.GetTimestamp() + (long)(_timeout.TotalSeconds * Stopwatch.Frequency);

            List<string> marks = MarksIn(binlogEvent);

            // Recorded before applying, so a delete's key is captured even though
            // the row will not exist afterwards.
            if (inside && TryGetRowsEventTableId(binlogEvent, out long tableId) &&
                _tableMaps.TryGetValue(tableId, out TableMapEvent? map) &&
                string.Equals(map.DatabaseName, _db, StringComparison.Ordinal) &&
                string.Equals(map.TableName, table.MySqlTable, StringComparison.Ordinal))
            {
                foreach ((Dictionary<string, object?>? before, Dictionary<string, object?>? after) in
                         RowChanges(binlogEvent, table))
                {
                    if (before is not null)
                        changed.Add(Keys.RowIdentity(before, table, true));
                    if (after is not null)
                        changed.Add(Keys.RowIdentity(after, table, true));
                }
            }

            await ApplyEventAsync(binlogEvent);

            if (marks.Contains(low, StringComparer.Ordinal))
                inside = true;
            if (marks.Contains(high, StringComparer.Ordinal))
                return changed;
            if (Program.StopRequested)
                throw new WindowInterruptedException("interrupted before the window closed");
            if (Stopwatch.GetTimestamp() > deadline)
            {
                throw new WindowInterruptedException(
                    "no binlog activity while waiting for a watermark - check that " +
                    _watermarkTable + " is in schema " + _db);
            }
        }

        throw new WindowInterruptedException("stream ended");
    }

    // -----------------------------------------------------------------------

    public async Task RunAsync()
    {
        // Top-level entry point: validate settings, discover column kinds, open
        // the binlog stream, backfill every table (phase 1), then follow the
        // binlog indefinitely once all tables are done (phase 2). Always closes
        // connections and saves state on the way out.
        try
        {
            await CheckSettingsAsync();
            foreach (Table table in _tables)
                await LoadKindsAsync(table);
            await OpenStreamAsync();

            foreach (Table table in _tables)
            {
                if (Program.StopRequested)
                    break;
                await BackfillAsync(table);
            }

            bool done = _tables.All(table => _state.Progress(table).Done);
            if (done && !Program.StopRequested)
            {
                Log.Info("backfill complete - following changes (Ctrl-C to stop)");
                await foreach (IBinlogEvent binlogEvent in StreamEventsAsync(_forceToken))
                {
                    await ApplyEventAsync(binlogEvent);
                    if (Program.StopRequested)
                    {
                        await CheckpointAsync();
                        return;
                    }
                }
            }
        }
        finally
        {
            await CloseAsync();
        }
    }

    private async Task CloseAsync()
    {
        // Best-effort shutdown: commit any pending PostgreSQL work (or roll it
        // back if the commit itself fails), close both connections, and persist
        // the final state so the next run knows where to resume.
        try
        {
            await _pgTransaction.CommitAsync(CancellationToken.None);
        }
        catch
        {
            try
            {
                await _pgTransaction.RollbackAsync(CancellationToken.None);
            }
            catch
            {
                // Best effort; a failed rollback is not worth aborting shutdown.
            }
        }

        await _pgTransaction.DisposeAsync();
        await _mysql.CloseAsync();
        await _postgres.CloseAsync();
        _state.Save();
        Log.Info("stopped at {0}:{1}", _state.LogFile ?? "",
            _state.LogPos?.ToString(CultureInfo.InvariantCulture) ?? "");
    }
}
