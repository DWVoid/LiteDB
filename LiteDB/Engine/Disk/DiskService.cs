using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using LiteDB.Storage;
using static LiteDB.Constants;

namespace LiteDB.Engine;

/// <summary>
/// Implement custom fast/in memory mapped disk access
/// [ThreadSafe]
/// </summary>
internal class DiskService : IDisposable
{
    private readonly IRandomAccess _dataFile;
    private readonly IRandomAccess _logFile;

    private readonly MemoryCache _cache;
    private readonly Lazy<DiskWriterQueue> _queue;

    private readonly IRandomAccessFactory _dataFactory;
    private readonly IRandomAccessFactory _logFactory;

    private long _dataLength;
    private long _logLength;

    public DiskService(
        EngineSettings settings,
        EngineState state,
        int[] memorySegmentSizes)
    {
        _cache = new MemoryCache(memorySegmentSizes);

        // get new stream factory based on settings
        _dataFactory = settings.CreateDataFileFactory();
        _logFactory = settings.CreateLogFileFactory();
        // TODO: this will actively open the datafile. refactor
        _dataFile = _dataFactory.Access;
        _logFile = _logFactory.Access;

        var isNew = _dataFactory.GetLength() == 0L;

        // create lazy async writer queue for log file
        _queue = new Lazy<DiskWriterQueue>(() => new DiskWriterQueue(_logFile, state));

        // create new database if not exist yet
        if (isNew)
        {
            LOG($"creating new database: '{Path.GetFileName(_dataFactory.Name)}'", "DISK");
            this.Initialize(_dataFile, settings.Collation);
        }

        // get initial data file length
        _dataLength = _dataFactory.GetLength() - PAGE_SIZE;

        // get initial log file length (should be 1 page before)
        if (_logFactory.Exists)
        {
            _logLength = _logFactory.GetLength() - PAGE_SIZE;
        }
        else
        {
            _logLength = -PAGE_SIZE;
        }
    }

    /// <summary>
    /// Get async queue writer
    /// </summary>
    public Lazy<DiskWriterQueue> Queue => _queue;

    /// <summary>
    /// Get memory cache instance
    /// </summary>
    // TODO: mark: cleanup - used WAL and system collection for stat reporting
    public MemoryCache Cache => _cache;

    /// <summary>
    /// Create a new empty database (use synced mode)
    /// </summary>
    // TODO: mark: cleanup - ctor helper
    private void Initialize(IRandomAccess stream, Collation collation)
    {
        var buffer = new PageBuffer(new byte[PAGE_SIZE], 0, 0);
        var header = new HeaderPage(buffer, 0);

        // update collation
        header.Pragmas.Set(Pragmas.COLLATION, (collation ?? Collation.Default).ToString(), false);

        // update buffer
        header.UpdateBuffer();
        stream.Write(buffer.Array.AsSpan().Slice(buffer.Offset, PAGE_SIZE), 0);
        stream.Flush();
    }

    /// <summary>
    /// Get a new instance for read data/log pages. This instance are not thread-safe - must request 1 per thread (used in Transaction)
    /// </summary>
    // TODO: mark: cleanup - used in transaction only, consider migration to WAL
    public DiskReader GetReader()
    {
        return new DiskReader(_cache, _dataFile, _logFile);
    }

    /// <summary>
    /// This method calculates the maximum number of items (documents or IndexNodes) that this database can have.
    /// The result is used to prevent infinite loops in case of problems with pointers
    /// Each page support max of 255 items. Use 10 pages offset (avoid empty disk)
    /// </summary>
    public uint MAX_ITEMS_COUNT => (uint)(((_dataLength + _logLength) / PAGE_SIZE) + 10) * byte.MaxValue;

    /// <summary>
    /// When a page are requested as Writable but not saved in disk, must be discard before release
    /// </summary>
    // TODO: mark: cleanup - used in transaction only, consider migration to WAL
    public void DiscardDirtyPages(IEnumerable<PageBuffer> pages)
    {
        // only for ROLLBACK action
        foreach (var page in pages)
        {
            // complete discard page and content
            _cache.DiscardPage(page);
        }
    }

    /// <summary>
    /// Discard pages that contains valid data and was not modified
    /// </summary>
    // TODO: mark: cleanup - used in transaction only, consider migration to WAL
    public void DiscardCleanPages(IEnumerable<PageBuffer> pages)
    {
        foreach (var page in pages)
        {
            // if page was not modified, try move to readable list
            if (_cache.TryMoveToReadable(page) == false)
            {
                // if already in readable list, just discard
                _cache.DiscardPage(page);
            }
        }
    }

    /// <summary>
    /// Request for a empty, writable non-linked page.
    /// </summary>
    // TODO: mark: cleanup - used in transaction only, consider migration to WAL
    public PageBuffer NewPage()
    {
        return _cache.NewPage();
    }

    /// <summary>
    /// Write pages inside file origin using async queue - WORKS ONLY FOR LOG FILE - returns how many pages are inside "pages"
    /// </summary>
    // TODO: mark: cleanup - used in transaction only, consider migration to WAL
    public int WriteAsync(IEnumerable<PageBuffer> pages)
    {
        var count = 0;

        foreach (var page in pages)
        {
            ENSURE(page.ShareCounter == BUFFER_WRITABLE, "to enqueue page, page must be writable");

            // adding this page into file AS new page (at end of file)
            // must add into cache to be sure that new readers can see this page
            page.Position = Interlocked.Add(ref _logLength, PAGE_SIZE);

            // should mark page origin to log because async queue works only for log file
            // if this page came from data file, must be changed before MoveToReadable
            page.Origin = FileOrigin.Log;

            // mark this page as readable and get cached paged to enqueue
            var readable = _cache.MoveToReadable(page);

            _queue.Value.EnqueuePage(readable);

            count++;
        }

        return count;
    }

    /// <summary>
    /// Get virtual file length: real file can be small because async thread can still writing on disk
    /// and incrementing file size (Log file)
    /// </summary>
    public long GetVirtualLength(FileOrigin origin) =>
        origin == FileOrigin.Log ? _logLength + PAGE_SIZE : _dataLength + PAGE_SIZE;

    /// <summary>
    /// Mark a file with a single signal to next open do auto-rebuild. Used only when closing database (after close files)
    /// </summary>
    // TODO: mark: cleanup - used in engine cleanup only, possible bad encapsulation
    internal void MarkAsInvalidState()
    {
        FileHelper.TryExec(60, () =>
        {
            try
            {
                var access = _dataFactory.Access;
                var buffer = new byte[PAGE_SIZE];
                access.Read(buffer, 0);
                buffer[HeaderPage.P_INVALID_DATAFILE_STATE] = 1;
                access.Write(buffer, 0);
            }
            catch
            {
                _dataFactory.Close();
            }
        });
    }

    #region Sync Read/Write operations

    /// <summary>
    /// Read all database pages inside file with no cache using. PageBuffers dont need to be Released
    /// </summary>
    // TODO: mark: cleanup - used in WAL and init only, need cleanup init misuse
    public IEnumerable<PageBuffer> ReadFull(FileOrigin origin)
    {
        var file = origin == FileOrigin.Log ? _logFile : _dataFile;
        // get length before starts (avoid grow during loop)
        var length = this.GetVirtualLength(origin);
        var fPosition = 0L;
        while (fPosition < length)
        {
            var buffer = new byte[PAGE_SIZE];
            var read = file.Read(buffer.AsSpan(), fPosition);
            ENSURE(read == PAGE_SIZE, $"ReadFull must read PAGE_SIZE bytes [{read}]");
            yield return new PageBuffer(buffer, 0, 0)
            {
                Position = fPosition,
                Origin = origin,
                ShareCounter = 0
            };
            fPosition += PAGE_SIZE;
        }
    }

    /// <summary>
    /// Write pages DIRECT in disk with NO queue. This pages are not cached and are not shared - WORKS FOR DATA FILE ONLY
    /// </summary>
    // TODO: mark: cleanup - used in WAL only
    public void Write(IEnumerable<PageBuffer> pages, FileOrigin origin)
    {
        ENSURE(origin == FileOrigin.Data);
        var file = origin == FileOrigin.Data ? _dataFile : _logFile;

        foreach (var page in pages)
        {
            ENSURE(page.ShareCounter == 0, "this page can't be shared to use sync operation - do not use cached pages");
            _dataLength = Math.Max(_dataLength, page.Position);
            file.Write(page.Array.AsSpan().Slice(page.Offset, PAGE_SIZE), page.Position);
        }

        file.Flush();
    }

    /// <summary>
    /// Set new length for file in sync mode. Queue must be empty before set length
    /// </summary>
    // TODO: mark: cleanup - used in WAL only
    public void SetLength(long length, FileOrigin origin)
    {
        var file = origin == FileOrigin.Log ? _logFile : _dataFile;
        ;
        if (origin == FileOrigin.Log)
        {
            ENSURE(_queue.Value.Length == 0, "queue must be empty before set new length");
            Interlocked.Exchange(ref _logLength, length - PAGE_SIZE);
        }
        else
        {
            Interlocked.Exchange(ref _dataLength, length - PAGE_SIZE);
        }

        file.Length = length;
    }

    /// <summary>
    /// Get file name (or Stream name)
    /// </summary>
    //  TODO: yes but why?
    public string GetName(FileOrigin origin)
    {
        return origin == FileOrigin.Data ? _dataFactory.Name : _logFactory.Name;
    }

    #endregion

    public void Dispose()
    {
        // dispose queue (wait finish)
        if (_queue.IsValueCreated) _queue.Value.Dispose();

        // get stream length from writer - is safe because only this instance
        // can change file size
        var delete = _logFactory.Exists && _logFile.Length == 0;

        // dispose file resources
        _dataFactory.Close();
        _logFactory.Close();

        if (delete) _logFactory.Delete();

        // other disposes
        _cache.Dispose();
    }
}