using System.Globalization;
using MySqlCdc;
using MySqlCdc.Events;
using MySqlConnector;
using Npgsql;

namespace DbRaidr;

/// <summary>
/// Owns both database connections plus all migration state, and drives the
/// two phases: BackfillAsync() per table, then a live ApplyEventAsync() loop.
///
/// Split across partial files:
///   Migration.cs          - fields, construction, RunAsync, CloseAsync
///   Migration.Setup.cs    - binlog settings, column kinds, stream open
///   Migration.Writes.cs   - checkpoint, upsert, delete
///   Migration.Events.cs   - apply binlog events
///   Migration.Backfill.cs - watermark window and chunked copy
/// </summary>
internal sealed partial class Migration
{
    // Source DDL that cannot be replayed onto PostgreSQL. Encountering one of
    // these on the configured schema aborts the run so the operator can apply
    // the change on both sides by hand.
    private static readonly HashSet<string> DdlWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"
    };

    // MySQL types that must stay as bytes. The charset=binary case is handled
    // separately in LoadKindsAsync so CHAR/VARCHAR BINARY also land here.
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

    // Table-map events name the table for later row events, which only carry
    // a numeric table id. The map is kept for the life of the stream.
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

    /// <summary>
    /// Top-level entry point: validate settings, discover column kinds, open
    /// the binlog stream, backfill every table (phase 1), then follow the
    /// binlog indefinitely once all tables are done (phase 2). Always closes
    /// connections and saves state on the way out.
    /// </summary>
    public async Task RunAsync()
    {
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

    /// <summary>
    /// Best-effort shutdown: commit any pending PostgreSQL work (or roll it
    /// back if the commit itself fails), close both connections, and persist
    /// the final state so the next run knows where to resume.
    /// </summary>
    private async Task CloseAsync()
    {
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
