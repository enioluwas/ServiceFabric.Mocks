using System.Collections;

namespace ServiceFabric.Mocks.ReliableCollections
{
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Implements IReliableQueue.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MockReliableQueue<T> : TransactedCollection, IReliableQueue<T>
    {
        private readonly IndexedQueue<T> _queue = new IndexedQueue<T>();
        private readonly Lock<long> _lock = new Lock<long>();
        private readonly IStateSerializer<T> _valueSerializer;

        protected long QueueCount => _queue.Count;

        public MockReliableQueue(Uri uri)
            : base(uri)
        { }

        internal MockReliableQueue(Uri uri, IReadOnlyStateSerializerStore serializerStore)
            : this(uri)
        {
            serializerStore.TryGetStateSerializer(out IStateSerializer<T> valueSerializer);
            this._valueSerializer = valueSerializer;
        }

        /// <summary>
        /// Clones <paramref name="value"/> if there is a value serializer registered for it. If not, returns the same value.
        /// Used to ensure that when the value is serializable, no direct reference to an object enters or leaves the reliable collection.
        /// </summary>
        /// <param name="value"></param>
        private T MaybeCloneValue(T value)
        {
            if (value != null && this._valueSerializer != null)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (BinaryWriter binaryWriter = new BinaryWriter(memoryStream))
                    using (BinaryReader binaryReader = new BinaryReader(memoryStream))
                    {
                        this._valueSerializer.Write(value, binaryWriter);
                        // Set the position back to the beginning of the stream before reading.
                        memoryStream.Position = 0;
                        return this._valueSerializer.Read(binaryReader);
                    }
                }
            }

            return value;
        }

        public override void ReleaseLocks(ITransaction tx)
        {
            _lock.Release(tx.TransactionId);
        }

        public Task ClearAsync()
        {
            _queue.Clear();

            return Task.FromResult(true);
        }

        public async Task<IAsyncEnumerable<T>> CreateEnumerableAsync(ITransaction tx)
        {
            await _lock.Acquire(BeginTransaction(tx).TransactionId, LockMode.Default, default(TimeSpan), CancellationToken.None);
            return new MockAsyncEnumerable<T>(_queue.Select((value) => MaybeCloneValue(value)));
        }

        public Task EnqueueAsync(ITransaction tx, T item)
        {
            return EnqueueAsync(tx, item, default(TimeSpan), CancellationToken.None);
        }

        public async Task EnqueueAsync(ITransaction tx, T item, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await _lock.Acquire(BeginTransaction(tx).TransactionId, LockMode.Update, timeout, cancellationToken);
            var itemToInsert = MaybeCloneValue(item);
            _queue.Enqueue(itemToInsert);
            AddAbortAction(tx, () => { _queue.Remove(itemToInsert); return true; });
        }

        public async Task<long> GetCountAsync(ITransaction tx)
        {
            await _lock.Acquire(BeginTransaction(tx).TransactionId, LockMode.Default, default(TimeSpan), CancellationToken.None);

            return _queue.Count;
        }

        public Task<ConditionalValue<T>> TryDequeueAsync(ITransaction tx)
        {
            return TryDequeueAsync(tx, default(TimeSpan), CancellationToken.None);
        }

        public async Task<ConditionalValue<T>> TryDequeueAsync(ITransaction tx, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await _lock.Acquire(BeginTransaction(tx).TransactionId, LockMode.Update, timeout, cancellationToken);
            if (_queue.Count > 0)
            {
                T item = _queue.Dequeue();
                AddAbortAction(tx, () => { _queue.AddToFront(item); return true; });

                return new ConditionalValue<T>(true, MaybeCloneValue(item));
            }

            return new ConditionalValue<T>();
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx)
        {
            return TryPeekAsync(tx, LockMode.Default);
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return TryPeekAsync(tx, LockMode.Default, timeout, cancellationToken);
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, LockMode lockMode)
        {
            return TryPeekAsync(tx, lockMode, default(TimeSpan), default(CancellationToken));
        }

        public async Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await _lock.Acquire(BeginTransaction(tx).TransactionId, lockMode, timeout, cancellationToken);
            if (_queue.Count > 0)
            {
                return new ConditionalValue<T>(true, MaybeCloneValue(_queue.Peek()));
            }

            return new ConditionalValue<T>();
        }
    }

    internal class IndexedQueue<T> : IEnumerable<T>
    {
        private readonly LinkedList<T> _inner = new LinkedList<T>();
        public long Count => _inner.Count;

        public void AddToFront(T element)
        {
            _inner.AddFirst(element);
        }


        public void Clear()
        {
            _inner.Clear();
        }

        public void Enqueue(T item)
        {
            _inner.AddLast(item);
        }

        public T Dequeue()
        {
            if (_inner.Count < 1) return default(T);
            var element = _inner.First.Value;
            _inner.RemoveFirst();
            return element;
        }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator()
        {
            return _inner.GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T Peek()
        {
            if (_inner.Count < 1) return default(T);
            return _inner.First.Value;
        }

        public void Remove(T item)
        {
            _inner.Remove(item);
        }
    }
}
