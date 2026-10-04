using System.Diagnostics;
using MySqlCdc.Events;

namespace DbRaidr;

/// <summary>
/// Phase 1: copy one table chunk by chunk using the DBLog watermark algorithm.
/// Change events that arrive while a chunk is being read are applied immediately;
/// snapshot rows whose keys changed in that window are discarded.
/// </summary>
internal sealed partial class Migration
{
    /// <summary>
    /// Insert a fresh, uniquely-identifiable marker row into the watermark
    /// table and return its id, so it can be recognized later in the stream.
    /// </summary>
    private async Task<string> WatermarkAsync()
    {
        string mark = Guid.NewGuid().ToString("N");
        await using var command = _mysql.CreateCommand();
        command.CommandText = "INSERT INTO " + SqlNames.QuoteMySql(_watermarkTable) +
                              " (id) VALUES (@id)";
        command.Parameters.AddWithValue("@id", mark);
        await command.ExecuteNonQueryAsync(_forceToken);
        return mark;
    }

    /// <summary>
    /// Row-constructor comparison, so composite keys of any comparable type
    /// work and we never assume keys are numeric or contiguous.
    /// </summary>
    private async Task<List<Dictionary<string, object?>>> FetchChunkAsync(
        Table table, List<object?>? afterKey)
    {
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

    /// <summary>
    /// Copy one table from MySQL to PostgreSQL, chunk by chunk, using the DBLog
    /// watermark algorithm to stay correct against concurrent writes. Resumes
    /// from entry.LastKey if a previous run left off partway, and returns
    /// without marking the table done if interrupted.
    /// </summary>
    private async Task BackfillAsync(Table table)
    {
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

    /// <summary>
    /// Apply events until the high watermark shows up, returning the keys of
    /// this table's rows that changed between the two watermarks.
    ///
    /// The timeout measures silence, not elapsed time: a wall-clock deadline
    /// would starve a busy table, discarding and re-reading the same chunk forever.
    /// </summary>
    private async Task<HashSet<RowKey>> FollowToAsync(Table table, string low, string high)
    {
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
}
