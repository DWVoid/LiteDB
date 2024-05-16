using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace LiteDB.Storage;

public class RandomAccessFile(SafeFileHandle handle) : IRandomAccessOwner
{
    ~RandomAccessFile() => Dispose();

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        handle.Dispose();
    }

    public long Length
    {
        get => RandomAccess.GetLength(handle);
        set => RandomAccess.SetLength(handle, value);
    }

    public int Read(Span<byte> buffer, long fileOffset) =>
        RandomAccess.Read(handle, buffer, fileOffset);

    public long Read(IReadOnlyList<Memory<byte>> buffers, long fileOffset) =>
        RandomAccess.Read(handle, buffers, fileOffset);

    public ValueTask<int> ReadAsync(Memory<byte> buffer, long fileOffset) =>
        RandomAccess.ReadAsync(handle, buffer, fileOffset);

    public ValueTask<long> ReadAsync(IReadOnlyList<Memory<byte>> buffers, long fileOffset) =>
        RandomAccess.ReadAsync(handle, buffers, fileOffset);

    public void Write(ReadOnlySpan<byte> buffer, long fileOffset) =>
        RandomAccess.Write(handle, buffer, fileOffset);

    public void Write(IReadOnlyList<ReadOnlyMemory<byte>> buffers, long fileOffset) =>
        RandomAccess.Write(handle, buffers, fileOffset);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, long fileOffset) =>
        RandomAccess.WriteAsync(handle, buffer, fileOffset);

    public ValueTask WriteAsync(IReadOnlyList<ReadOnlyMemory<byte>> buffers, long fileOffset) =>
        RandomAccess.WriteAsync(handle, buffers, fileOffset);

    public void Flush() => RandomAccess.FlushToDisk(handle);
}

public class RandomAccessFileFactory(string filename, bool readOnly) : IRandomAccessFactory
{
    private RandomAccessFile _file;

    public string Name => filename;

    public bool Exists
    {
        get
        {
            lock (this) return _file != null || File.Exists(filename);
        }
    }

    public IRandomAccess Access
    {
        get
        {
            lock (this)
            {
                if (_file != null) return _file;
                _file = new RandomAccessFile(
                    File.OpenHandle(
                        filename,
                        FileMode.OpenOrCreate,
                        readOnly ? FileAccess.Read : FileAccess.ReadWrite,
                        readOnly ? FileShare.Read : FileShare.ReadWrite
                    )
                );
                return _file;
            }
        }
    }

    public void Close()
    {
        lock (this)
        {
            if (_file == null) return;
            _file.Dispose();
            _file = null;
        }
    }

    public void Delete()
    {
        lock (this)
        {
            if (_file != null)
            {
                _file.Dispose();
                _file = null;
            }

            File.Delete(filename);
        }
    }

    public long GetLength()
    {
        lock (this)
        {
            if (_file != null) return _file.Length;
            var fi = new FileInfo(filename);
            return fi.Exists ? fi.Length : 0;
        }
    }
}