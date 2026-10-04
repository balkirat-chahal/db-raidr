using System.Runtime.InteropServices;
using System.Text.Json;

namespace DbRaidr;

/// <summary>
/// Entry point for DB-RAIDr: a MySQL -&gt; PostgreSQL copy using the DBLog
/// watermark algorithm.
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
    // First Ctrl-C sets this so the current chunk can finish. A second Ctrl-C
    // sets _forcedExitRequested and cancels ForceExit, which unblocks the stream.
    private static volatile bool _stopRequested;
    private static volatile bool _forcedExitRequested;
    private static readonly CancellationTokenSource ForceExit = new();
    private static PosixSignalRegistration? _sigtermRegistration;

    public static async Task<int> Main(string[] args)
    {
        // Load .env before reading passwords so a local file can supply them
        // without exporting anything in the shell.
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
            // Second interrupt: the stream was cancelled mid-event. Position
            // saved on disk may be slightly behind what MySQL already sent.
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

    /// <summary>
    /// True after the first interrupt. Backfill and follow loops check this
    /// between units of work so they can checkpoint and exit cleanly.
    /// </summary>
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

    /// <summary>
    /// Read a password from the environment variable named in config.json.
    /// Kept out of the config file so the config is safe to commit. An empty
    /// value is accepted; an unset variable is almost always a forgotten export.
    /// </summary>
    internal static string ReadPassword(string envVar)
    {
        string? password = Environment.GetEnvironmentVariable(envVar);
        if (password is null)
            throw new InvalidOperationException(
                $"Failed reading password - environment variable '{envVar}' is not set");
        return password;
    }

    /// <summary>
    /// Shared JSON options for config load and atomic state-file writes.
    /// Property names are case-sensitive so they match config.json exactly.
    /// </summary>
    internal static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = false,
        WriteIndented = true
    };
}
