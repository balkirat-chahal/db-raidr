namespace DbRaidr;

/// <summary>
/// A chunk's watermark window could not be completed: the process was
/// interrupted, the stream ended, or the binlog went silent past the timeout.
///
/// The backfill loop catches this, checkpoints whatever change events were
/// already applied, and returns without advancing last_key. The next run
/// re-reads the same chunk. Nothing is lost, because change events are always
/// applied even when the snapshot copy of the chunk is thrown away.
/// </summary>
internal sealed class WindowInterruptedException : Exception
{
    public WindowInterruptedException(string message) : base(message) { }
}
