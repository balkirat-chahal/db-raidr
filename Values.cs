using System.Collections;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using MySqlCdc.Providers.MySql;
using MySqlConnector;

namespace DbRaidr;

/// <summary>
/// Canonical conversion of a single cell.
///
/// The same row reaches us twice by two different routes: once from a SELECT
/// (MySqlConnector) and once from the binlog (MySqlCdc). The two libraries do
/// not always return the same CLR object for the same column, so every value is
/// put into one canonical form based on what information_schema says the column is.
/// </summary>
internal static class Values
{
    /// <summary>
    /// Decode bytes to string; anything else passes through unchanged.
    /// </summary>
    public static object? ToText(object? value)
    {
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            Memory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            _ => value
        };
    }

    /// <summary>
    /// Encode a string to bytes; byte-like values pass through unchanged.
    /// Anything else would lose information, so it is rejected.
    /// </summary>
    public static byte[] ToBytes(object value)
    {
        return value switch
        {
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            Memory<byte> memory => memory.ToArray(),
            string text => Encoding.UTF8.GetBytes(text),
            _ => throw new InvalidOperationException(
                $"cannot coerce {value.GetType().Name} to bytes without losing information")
        };
    }

    /// <summary>
    /// A SELECT and the binlog represent BIT differently. Convert both to one
    /// integer representation so key identity and PostgreSQL writes agree.
    /// </summary>
    public static object BitToInteger(object value)
    {
        if (value is bool boolean)
            return boolean ? 1L : 0L;

        if (value is bool[] bits)
        {
            BigInteger result = BigInteger.Zero;
            foreach (bool bit in bits)
                result = (result << 1) | (bit ? BigInteger.One : BigInteger.Zero);
            return NarrowBigInteger(result);
        }

        if (value is BitArray bitArray)
        {
            BigInteger result = BigInteger.Zero;
            for (int i = 0; i < bitArray.Length; i++)
                result = (result << 1) | (bitArray[i] ? BigInteger.One : BigInteger.Zero);
            return NarrowBigInteger(result);
        }

        if (value is byte[] bytes)
        {
            BigInteger result = BigInteger.Zero;
            foreach (byte item in bytes)
                result = (result << 8) | item;
            return NarrowBigInteger(result);
        }

        return value;
    }

    /// <summary>
    /// Prefer a 64-bit integer when the BIT value fits; keep BigInteger only
    /// for wider masks so key encoding stays compact for the common case.
    /// </summary>
    private static object NarrowBigInteger(BigInteger value)
    {
        if (value >= long.MinValue && value <= long.MaxValue)
            return (long)value;
        return value;
    }

    /// <summary>
    /// Put a raw cell into the form used for key identity and PostgreSQL writes.
    /// <paramref name="fromBinlog"/> selects the extra signedness / JSON fixes
    /// that only the replication library needs.
    /// </summary>
    public static object? CanonicalSourceValue(object? value, ColumnInfo info, bool fromBinlog)
    {
        if (value is null || value is DBNull)
            return null;

        // MySqlCdc needs help with a few types whose raw representation differs
        // from MySqlConnector. Normalize those differences before key comparison.
        value = NormalizeSignedness(value, info, fromBinlog);

        if (info.Kind == "binary")
            return ToBytes(value);
        if (info.Kind == "bit")
            return BitToInteger(value);
        if (info.Kind == "json")
            return JsonToText(value, fromBinlog);
        if (info.Kind == "set")
            return SetToText(value, info.TypeMembers);
        if (info.Kind == "enum")
            return EnumToText(value, info.TypeMembers);
        if (info.Kind == "decimal")
            return DecimalToText(value);

        if (info.SourceType == "date")
        {
            if (value is DateTime dateTime)
                return DateOnly.FromDateTime(dateTime);
            return value;
        }

        // MySqlCdc exposes TIMESTAMP as DateTimeOffset while normal SELECTs are
        // DateTime. Put both on a timezone-free DateTime so the two routes agree.
        if (info.SourceType == "timestamp" && value is DateTimeOffset dto)
            return DateTime.SpecifyKind(dto.UtcDateTime, DateTimeKind.Unspecified);

        return value;
    }

    /// <summary>
    /// One MySQL value -&gt; something Npgsql can send.
    /// </summary>
    public static object? ConvertForPostgres(object? value, ColumnInfo info, bool fromBinlog)
    {
        value = CanonicalSourceValue(value, info, fromBinlog);
        if (value is null)
            return null;

        if (info.Kind is "binary" or "bit" or "json" or "set" or "enum" or "decimal")
            return value;

        if (value is byte[] bytes)
            return Encoding.UTF8.GetString(bytes);

        if (value is TimeSpan duration)
            return FormatMySqlTime(duration); // MySQL TIME

        if (value is MySqlDecimal mysqlDecimal)
            return mysqlDecimal.ToString();

        return value;
    }

    /// <summary>
    /// MySqlCdc documents integer storage in fixed CLR types. Reinterpret the
    /// raw bits for UNSIGNED columns when necessary, and keep signed tinyints
    /// signed so SELECT and binlog values compare the same.
    /// Snapshot SELECTs already have the correct signedness, so they skip this.
    /// </summary>
    private static object NormalizeSignedness(object value, ColumnInfo info, bool fromBinlog)
    {
        if (!fromBinlog)
            return value;

        return info.SourceType switch
        {
            "tinyint" when info.Unsigned && value is sbyte v => unchecked((byte)v),
            "tinyint" when !info.Unsigned && value is byte v => unchecked((sbyte)v),
            "smallint" when info.Unsigned && value is short v => unchecked((ushort)v),
            "mediumint" when info.Unsigned && value is int v => ((uint)v << 8) >> 8,
            "int" when info.Unsigned && value is int v => unchecked((uint)v),
            "bigint" when info.Unsigned && value is long v => unchecked((ulong)v),
            _ => value
        };
    }

    /// <summary>
    /// A SELECT gives JSON already serialized. MySqlCdc gives MySQL JSON as
    /// its binary on-wire representation, so parse it back to valid JSON text.
    /// </summary>
    private static string JsonToText(object value, bool fromBinlog)
    {
        if (!fromBinlog)
            return Convert.ToString(ToText(value), CultureInfo.InvariantCulture) ?? "null";

        if (value is byte[] bytes)
            return JsonParser.Parse(bytes);
        if (value is ReadOnlyMemory<byte> memory)
            return JsonParser.Parse(memory.ToArray());
        if (value is string text)
            return text;

        return JsonSerializer.Serialize(value);
    }

    /// <summary>
    /// MySQL SET: SELECT gives "a,b"; MySqlCdc gives the numeric bit mask.
    /// Sorting both routes is the point: the row does not change value based
    /// on which route it arrived through.
    /// </summary>
    private static string SetToText(object value, IReadOnlyList<string> members)
    {
        IEnumerable<string> values;
        if (value is string text)
        {
            values = text.Length == 0 ? [] : text.Split(',');
        }
        else if (value is byte[] bytes)
        {
            string textValue = Encoding.UTF8.GetString(bytes);
            values = textValue.Length == 0 ? [] : textValue.Split(',');
        }
        else if (TryUnsignedInteger(value, out ulong mask))
        {
            var selected = new List<string>();
            for (int i = 0; i < members.Count && i < 64; i++)
            {
                if ((mask & (1UL << i)) != 0)
                    selected.Add(members[i]);
            }
            values = selected;
        }
        else if (value is IEnumerable enumerable)
        {
            var selected = new List<string>();
            foreach (object? item in enumerable)
                selected.Add(Convert.ToString(ToText(item), CultureInfo.InvariantCulture) ?? string.Empty);
            values = selected;
        }
        else
        {
            values = [Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty];
        }

        return string.Join(',', values.OrderBy(value => value, StringComparer.Ordinal));
    }

    /// <summary>
    /// MySqlConnector SELECTs return the label; MySqlCdc represents ENUM by
    /// its 1-based numeric index. Convert the binlog form back to the label.
    /// Index 0 is MySQL's empty ENUM (invalid / unset).
    /// </summary>
    private static string EnumToText(object value, IReadOnlyList<string> members)
    {
        if (value is string text)
            return text;
        if (value is byte[] bytes)
            return Encoding.UTF8.GetString(bytes);

        if (TryUnsignedInteger(value, out ulong index))
        {
            if (index == 0)
                return string.Empty;
            if (index <= (ulong)members.Count)
                return members[(int)index - 1];
        }

        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>
    /// Keep DECIMAL as invariant text so precision is not rounded by a CLR type.
    /// </summary>
    private static string DecimalToText(object value)
    {
        return value switch
        {
            MySqlDecimal mysqlDecimal => mysqlDecimal.ToString(),
            decimal number => number.ToString(CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };
    }

    /// <summary>
    /// Treat any integer-width CLR type as an unsigned bit mask / enum index.
    /// Signed values are reinterpreted, matching how the binlog stores them.
    /// </summary>
    private static bool TryUnsignedInteger(object value, out ulong result)
    {
        switch (value)
        {
            case byte v: result = v; return true;
            case sbyte v: result = unchecked((ulong)v); return true;
            case ushort v: result = v; return true;
            case short v: result = unchecked((ulong)v); return true;
            case uint v: result = v; return true;
            case int v: result = unchecked((ulong)v); return true;
            case ulong v: result = v; return true;
            case long v: result = unchecked((ulong)v); return true;
            default:
                result = 0;
                return false;
        }
    }

    /// <summary>
    /// Format MySQL TIME as days, hours, minutes, seconds, and microseconds,
    /// including negative-value normalization (days go more negative while
    /// the time-of-day remainder stays non-negative).
    /// </summary>
    private static string FormatMySqlTime(TimeSpan value)
    {
        const long microsPerSecond = 1_000_000;
        const long microsPerDay = 86_400 * microsPerSecond;

        long totalMicros = value.Ticks / 10;
        long days = FloorDiv(totalMicros, microsPerDay);
        long remainder = totalMicros - days * microsPerDay;

        long hours = remainder / (3_600 * microsPerSecond);
        remainder %= 3_600 * microsPerSecond;
        long minutes = remainder / (60 * microsPerSecond);
        remainder %= 60 * microsPerSecond;
        long seconds = remainder / microsPerSecond;
        long micros = remainder % microsPerSecond;

        string time = micros == 0
            ? $"{hours}:{minutes:00}:{seconds:00}"
            : $"{hours}:{minutes:00}:{seconds:00}.{micros:000000}";

        if (days == 0)
            return time;
        return $"{days} day{(Math.Abs(days) == 1 ? string.Empty : "s")}, {time}";
    }

    /// <summary>
    /// Integer division that rounds toward negative infinity, so a negative
    /// TIME still splits into (more-negative days) + (non-negative remainder).
    /// </summary>
    private static long FloorDiv(long value, long divisor)
    {
        long quotient = value / divisor;
        long remainder = value % divisor;
        if (remainder != 0 && value < 0)
            quotient--;
        return quotient;
    }
}
