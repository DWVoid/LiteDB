using System;
using LiteDB.Storage;
using static LiteDB.Constants;

namespace LiteDB.Engine;

/// <summary>
/// Memory file reader - must call Dipose after use to return reader into pool
/// This class is not ThreadSafe - must have 1 instance per thread (get instance from DiskService)
/// </summary>
internal class DiskReader(MemoryCache cache, IRandomAccess dataFile, IRandomAccess logFile) : IDisposable
{
    public PageBuffer ReadPage(long position, bool writable, FileOrigin origin)
    {
        ENSURE(position % PAGE_SIZE == 0, "invalid page position");
        var stream = origin == FileOrigin.Data ? dataFile : logFile;
        var page = writable
            ? cache.GetWritablePage(position, origin, (pos, buf) => this.ReadFile(stream, pos, buf))
            : cache.GetReadablePage(position, origin, (pos, buf) => this.ReadFile(stream, pos, buf));
#if DEBUG
        _state.SimulateDiskReadFail?.Invoke(page);
#endif
        return page;
    }

    /// <summary>
    /// Read bytes from stream into buffer slice
    /// </summary>
    private void ReadFile(IRandomAccess origin, long position, BufferSlice buffer)
    {
        // can't test "Length" from out-to-date stream
        // ENSURE(stream.Length <= position - PAGE_SIZE, "can't be read from beyond file length");
        var read = origin.Read(buffer.Array.AsSpan().Slice(buffer.Offset, buffer.Count), position);
        ENSURE(read == buffer.Count);
        DEBUG(buffer.All(0) == false, "check if are not reading out of file length");
    }

    /// <summary>
    /// Request for a empty, writable non-linked page (same as DiskService.NewPage)
    /// </summary>
    public PageBuffer NewPage() => cache.NewPage();

    /// <summary>
    /// When dispose, return stream to pool
    /// </summary>
    public void Dispose()
    {
    }
}