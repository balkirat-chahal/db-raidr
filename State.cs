using System.Globalization;
using System.Text.Json;

namespace DbRaidr;

/// <summary>
/// Per-table backfill progress stored in the state file: whether the table is
/// done, the last key of the last fully processed chunk, and the key_columns
/// that last_key was encoded with (so a config change cannot silently reuse it).
/// </summary>
internal sealed class TableProgress
{
    public bool Done { get; set; }
    public List<EncodedValue>? LastKey { get; set; }
    public List<string> KeyColumns { get; set; } = [];
}

/// <summary>
/// Binlog position plus per-table progress, written atomically.
///
/// The file lives in the workspace directory. A crash mid-write cannot leave it
/// half-written: Save() writes a sibling .tmp file, fsyncs, then renames over
/// the real path.
/// </summary>
internal sealed class State
{
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

    /// <summary>
    /// Write state to a temp file and rename over the real path, so a crash
    /// mid-write never leaves a half-written, unreadable state file.
    /// </summary>
    public void Save()
    {
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

    /// <summary>
    /// Get (or create) this table's progress entry. Refuse to reuse a saved
    /// entry whose key_columns no longer match the config, since last_key would
    /// then mean something different.
    /// </summary>
    public TableProgress Progress(Table table)
    {
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
