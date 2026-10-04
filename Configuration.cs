using System.Text.Json.Serialization;

namespace DbRaidr;

/// <summary>
/// Root object of config.json. Defaults match the documented fields so a
/// minimal file only has to name the two databases and the table list.
/// </summary>
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

/// <summary>
/// Connection settings for one database. The password is never stored here;
/// <see cref="PasswordEnv"/> names the environment variable that holds it.
/// A zero port is treated as the engine default (3306 / 5432) at connect time.
/// </summary>
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

/// <summary>
/// One entry of the config.json "tables" list: source name, target name, the
/// unique key used for pagination and upserts, and optional skipped columns.
/// </summary>
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
