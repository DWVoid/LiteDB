using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LiteDB.Storage;
using Microsoft.Win32.SafeHandles;
using static LiteDB.Constants;

namespace LiteDB.Engine;

/// <summary>
/// Implement disk write queue and async writer thread - used only for write on LOG file
/// [ThreadSafe]
/// </summary>
internal class DiskWriterQueue : IDisposable
{
    private readonly IRandomAccess _file;
    private readonly EngineState _state;

    // async thread controls
    private Task _task;

    private readonly Channel<PageBuffer> _channel = Channel.CreateUnbounded<PageBuffer>();
    private readonly ManualResetEventSlim _queueIsEmpty = new(true);

    private Exception _exception = null; // store last exception in async running task

    public DiskWriterQueue(IRandomAccess file, EngineState state)
    {
        _file = file;
        _state = state;
        _task = Task.Run(ExecuteQueue);
    }

    /// <summary>
    /// Get how many pages are waiting for store
    /// </summary>
    public int Length => _channel.Reader.Count;

    /// <summary>
    /// Add page into writer queue and will be saved in disk by another thread. If page.Position = MaxValue, store at end of file (will get final Position)
    /// After this method, this page will be available into reader as a clean page
    /// </summary>
    public void EnqueuePage(PageBuffer page)
    {
        ENSURE(page.Origin == FileOrigin.Log, "async writer must use only for Log file");

        // throw last exception that stop running queue
        if (_exception != null) throw _exception;

        lock (_channel)
        {
            _queueIsEmpty.Reset();
            ENSURE(_channel.Writer.TryWrite(page), "enqueue failure on unbounded queue");
        }
    }

    /// <summary>
    /// Wait until all queue be executed and no more pending pages are waiting for write - be sure you do a full lock database before call this
    /// </summary>
    public void Wait()
    {
        _queueIsEmpty.Wait();
        ENSURE(_channel.Reader.Count == 0, "queue should be empty after wait() call");
    }

    /// <summary>
    /// Execute all items in queue sync
    /// </summary>
    private async Task ExecuteQueue()
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync())
            {
                while (_channel.Reader.TryRead(out var page)) WritePageToStream(page);
                lock (_channel)
                {
                    if (_channel.Reader.Count > 0) continue;
                    _queueIsEmpty.Set();
                }

                _file.Flush();
            }
        }
        catch (Exception ex)
        {
            _state.Handle(ex);
            _exception = ex;
        }
    }

    private void WritePageToStream(PageBuffer page)
    {
        if (page == null) return;
        ENSURE(page.ShareCounter > 0, "page must be shared at least 1");
#if DEBUG
        _state.SimulateDiskWriteFail?.Invoke(page);
#endif
        _file.Write(page.Array.AsSpan().Slice(page.Offset, PAGE_SIZE), page.Position);
        // release page here (no page use after this)
        page.Release();
    }

    public void Dispose()
    {
        LOG($"disposing disk writer queue (with {_channel.Reader.Count} pages in queue)", "DISK");
        _channel.Writer.Complete(); // unblock the running loop in case there are no items
        _task?.Wait();
        _task = null;
    }
}