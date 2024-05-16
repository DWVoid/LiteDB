using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LiteDB.Storage;

public interface IRandomAccess
{
    public long Length { get; set; }
    public int Read(Span<byte> buffer, long fileOffset);
    public long Read(IReadOnlyList<Memory<byte>> buffers, long fileOffset);
    public ValueTask<int> ReadAsync(Memory<byte> buffer, long fileOffset);
    public ValueTask<long> ReadAsync(IReadOnlyList<Memory<byte>> buffers, long fileOffset);
    public void Write(ReadOnlySpan<byte> buffer, long fileOffset);
    public void Write(IReadOnlyList<ReadOnlyMemory<byte>> buffers, long fileOffset);
    public ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, long fileOffset);
    public ValueTask WriteAsync(IReadOnlyList<ReadOnlyMemory<byte>> buffers, long fileOffset);
    public void Flush();
}

public interface IRandomAccessOwner : IRandomAccess, IDisposable;

public interface IRandomAccessFactory
{
    public string Name { get; }
    public bool Exists { get; }
    IRandomAccess Access { get; }
    public void Close();
    public void Delete();
    long GetLength();
}