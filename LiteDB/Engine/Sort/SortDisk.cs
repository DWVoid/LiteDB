using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LiteDB.Storage;
using static LiteDB.Constants;

namespace LiteDB.Engine
{
    /// <summary>
    /// Single instance of TempDisk manage read/write access to temporary disk - used in merge sort
    /// [ThreadSafe]
    /// </summary>
    internal class SortDisk : IDisposable
    {
        private readonly IRandomAccessFactory _factory;
        private readonly ConcurrentBag<long> _freePositions = new ConcurrentBag<long>();
        private long _lastContainerPosition = 0;
        private readonly int _containerSize;
        private readonly EnginePragmas _pragmas;
        private object _wLock = new();

        public int ContainerSize => _containerSize;

        public SortDisk(IRandomAccessFactory factory, int containerSize, EnginePragmas pragmas)
        {
            ENSURE(containerSize % PAGE_SIZE == 0, "size must be PAGE_SIZE multiple");

            _factory = factory;
            _containerSize = containerSize;
            _pragmas = pragmas;

            _lastContainerPosition = -containerSize;
        }

        /// <summary>
        /// Get a new reader stream from pool. Must return after use
        /// </summary>
        public IRandomAccess GetReader()
        {
            return _factory.Access;
        }

        /// <summary>
        /// Return used open reader stream to be reused in next sort
        /// </summary>
        public void Return(IRandomAccess stream)
        {
        }

        /// <summary>
        /// Return used disk container position to be reused in next sort
        /// </summary>
        public void Return(long position)
        {
            _freePositions.Add(position);
        }

        /// <summary>
        /// Get next avaiable disk position - can be a new extend file or reuse container slot
        /// Use thread safe classes to ensure multiple threads access at same time
        /// </summary>
        public long GetContainerPosition()
        {
            if (_freePositions.TryTake(out var position))
            {
                return position;
            }

            position = Interlocked.Add(ref _lastContainerPosition, _containerSize);

            return position;
        }

        /// <summary>
        /// Write buffer container data into disk
        /// </summary>
        public void Write(long position, BufferSlice buffer)
        {
            var access = _factory.Access;

            // there is only a single writer instance, must be lock to ensure only 1 single thread are writing
            lock(_wLock)
            {
                // TODO: simplify
                for (var i = 0; i < _containerSize / PAGE_SIZE; ++i)
                {
                    access.Write(
                        buffer.Array.AsSpan().Slice(buffer.Offset + i * PAGE_SIZE, PAGE_SIZE),
                        position + i * PAGE_SIZE
                    );
                }
            }
        }

        public void Dispose()
        {
            _factory.Delete();
        }
    }
}
