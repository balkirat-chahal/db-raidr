namespace DbRaidr;

/// <summary>
/// Per-column metadata discovered from information_schema at startup.
/// <see cref="Kind"/> drives conversion (plain / binary / bit / json / set /
/// enum / decimal). <see cref="SourceType"/> is the raw DATA_TYPE. SET and ENUM
/// members come from COLUMN_TYPE so binlog numeric values can be turned back
/// into labels.
/// </summary>
internal sealed class ColumnInfo
{
    public required string Name { get; init; }
    public required string Kind { get; init; }
    public required string SourceType { get; init; }
    public bool Unsigned { get; init; }
    public IReadOnlyList<string> TypeMembers { get; init; } = [];
}
