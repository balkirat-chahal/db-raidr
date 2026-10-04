using System.Globalization;
using MySqlCdc.Events;

namespace DbRaidr;

/// <summary>
/// Apply binlog events to PostgreSQL. Change events are applied unconditionally,
/// window open or not. Rows go one at a time, in binlog order: batching would
/// reorder operations on the same key and produce the wrong final state.
/// </summary>
internal sealed partial class Migration
{
    /// <summary>
    /// The watermark ids inserted by this event, if it is a write to the
    /// watermark table; otherwise an empty list. FollowToAsync uses this to
    /// spot the low/high markers as they come back through the binlog stream.
    /// </summary>
    private List<string> MarksIn(IBinlogEvent binlogEvent)
    {
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

    /// <summary>
    /// Change events are applied unconditionally, window open or not.
    /// Rows go one at a time, in binlog order. Batching would reorder
    /// operations on the same key and produce the wrong final state.
    /// </summary>
    private async Task ApplyEventAsync(IBinlogEvent binlogEvent)
    {
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

    /// <summary>
    /// True if this QueryEvent's default database is the migrated schema, or
    /// if the SQL text names that schema explicitly. MySqlCdc streams all
    /// server schemas, so unrelated DDL must not abort the run.
    /// </summary>
    private bool QueryTouchesTargetSchema(QueryEvent queryEvent, string query)
    {
        if (string.Equals(queryEvent.DatabaseName, _db, StringComparison.Ordinal))
            return true;

        return query.Contains("`" + _db.Replace("`", "``") + "`.", StringComparison.OrdinalIgnoreCase)
               || query.Contains(_db + ".", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Write/Update/Delete row events each expose TableId, but they do not
    /// share a library base type. Pattern-match the three concrete types.
    /// </summary>
    private static bool TryGetRowsEventTableId(IBinlogEvent binlogEvent, out long tableId)
    {
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

    /// <summary>
    /// (before, after) pairs. Insert has no before, delete has no after.
    /// </summary>
    private IEnumerable<(Dictionary<string, object?>? Before, Dictionary<string, object?>? After)>
        RowChanges(IBinlogEvent binlogEvent, Table table)
    {
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

    /// <summary>
    /// Zip binlog cells onto information_schema column names. A length mismatch
    /// means the server is not logging FULL row images/metadata.
    /// </summary>
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

    /// <summary>
    /// Apply conversion to every column of a row, dropping skipped columns.
    /// </summary>
    private static Dictionary<string, object?> ConvertRow(
        Dictionary<string, object?> row, Table table, bool fromBinlog)
    {
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
}
