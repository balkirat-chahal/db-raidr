namespace DbRaidr;

/// <summary>
/// Quote SQL identifiers so table and column names with reserved words,
/// mixed case, or punctuation survive as written.
/// </summary>
internal static class SqlNames
{
    /// <summary>
    /// Double-quote a PostgreSQL identifier, escaping embedded quotes. Handles
    /// "schema.table" by quoting each dot-separated part on its own.
    /// </summary>
    public static string QuotePg(string name)
    {
        return string.Join('.', name.Split('.').Select(part =>
            "\"" + part.Replace("\"", "\"\"") + "\""));
    }

    /// <summary>
    /// Backtick-quote a MySQL identifier, escaping embedded backticks.
    /// </summary>
    public static string QuoteMySql(string name)
    {
        return "`" + name.Replace("`", "``") + "`";
    }
}
