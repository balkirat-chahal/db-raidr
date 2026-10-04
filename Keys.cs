using System.Globalization;
using System.Numerics;
using MySqlConnector;

namespace DbRaidr;

/// <summary>
/// One component of a row key: a type tag plus a text encoding that is both
/// JSON-serializable (for the state file) and comparable (for HashSet lookup).
/// The type name is part of the identity, so 5 and "5" never compare equal.
/// </summary>
internal readonly record struct EncodedValue(string Type, string Text);

/// <summary>
/// The comparable identity of one row: the encoded key columns, in config order.
///
/// This has to be exactly right. If the snapshot copy and the binlog copy of one
/// row do not produce the same key, the stale snapshot row is not discarded and
/// it overwrites the newer change event. That is silent data loss, and it only
/// affects rows written during the backfill.
/// </summary>
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

/// <summary>
/// Encode, decode, and compare row keys.
///
/// Binary keys are the dangerous case: random bytes are usually not valid UTF-8,
/// so decoding them loses information. We hex them instead, which is reversible.
/// </summary>
internal static class Keys
{
    /// <summary>
    /// Put a raw key value into the canonical form used for identity
    /// comparisons before it is handed to EncodeValue().
    /// </summary>
    public static object? NormalizeKey(object? value, ColumnInfo info, bool fromBinlog)
    {
        value = Values.CanonicalSourceValue(value, info, fromBinlog);
        if (value is null)
            return null;
        if (info.Kind == "binary")
            return Values.ToBytes(value);
        if (info.Kind == "bit")
            return Values.BitToInteger(value);
        return Values.ToText(value);
    }

    /// <summary>
    /// Turn a normalized key value into a (type, text) pair that is both
    /// JSON-serializable and hashable/comparable, and DecodeValue can rebuild.
    /// </summary>
    public static EncodedValue EncodeValue(object? value)
    {
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

    /// <summary>
    /// Reverse of EncodeValue(), used when resuming a backfill from state.
    /// </summary>
    public static object? DecodeValue(EncodedValue pair)
    {
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

    /// <summary>
    /// Integers in the state file are stored as decimal text. Prefer Int64,
    /// then UInt64, then BigInteger so unsigned BIGINT still round-trips.
    /// </summary>
    private static object DecodeInteger(string text)
    {
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long signed))
            return signed;
        if (ulong.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong unsigned))
            return unsigned;
        return BigInteger.Parse(text, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The comparable identity of a row, whichever route it came by.
    /// </summary>
    public static RowKey RowIdentity(Dictionary<string, object?> row, Table table, bool fromBinlog)
    {
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
