namespace DbRaidr;

/// <summary>
/// One table's migration config, plus column kinds discovered later from
/// information_schema (see <c>Migration.LoadKindsAsync</c>).
///
/// <see cref="Columns"/> is in ORDINAL_POSITION order so binlog cell lists
/// line up with names. <see cref="SkipColumns"/> are omitted on PostgreSQL
/// writes (generated columns, search vectors, and similar).
/// </summary>
internal sealed class Table
{
    public string MySqlTable { get; }
    public string PgTable { get; }
    public IReadOnlyList<string> KeyColumns { get; }
    public HashSet<string> SkipColumns { get; }
    public List<string> Columns { get; } = [];
    private readonly Dictionary<string, ColumnInfo> _columnInfo = new(StringComparer.Ordinal);

    /// <param name="raw">One entry of the "tables" list from config.json.</param>
    public Table(TableConfig raw)
    {
        MySqlTable = raw.MySqlTable;
        PgTable = raw.PgTable;
        KeyColumns = raw.KeyColumns;
        SkipColumns = new HashSet<string>(raw.SkipColumns, StringComparer.Ordinal);

        if (KeyColumns.Count == 0)
            throw new InvalidOperationException(MySqlTable + ": key_columns is empty");
        if (KeyColumns.Any(SkipColumns.Contains))
            throw new InvalidOperationException(MySqlTable + ": a key column is skipped");
    }

    /// <summary>
    /// Record a column discovered from information_schema. Called in ordinal
    /// order so <see cref="Columns"/> matches binlog cell order.
    /// </summary>
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
