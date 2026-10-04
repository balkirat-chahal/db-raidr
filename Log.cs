using System.Globalization;

namespace DbRaidr;

/// <summary>
/// Timestamped stderr logger. Progress, warnings, and errors all go to stderr
/// so stdout stays free if a caller ever wants to compose this process.
/// Format is "HH:mm:ss LEVEL   message", matching the original console output.
/// </summary>
internal static class Log
{
    public static void Info(string message, params object?[] args) => Write("INFO", message, args);
    public static void Warning(string message, params object?[] args) => Write("WARNING", message, args);
    public static void Error(string message, params object?[] args) => Write("ERROR", message, args);

    private static void Write(string level, string message, params object?[] args)
    {
        string text = args.Length == 0
            ? message
            : string.Format(CultureInfo.InvariantCulture, message, args);
        Console.Error.WriteLine($"{DateTime.Now:HH:mm:ss} {level,-7} {text}");
    }
}
