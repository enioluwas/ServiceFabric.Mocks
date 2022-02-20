﻿namespace ServiceFabric.Mocks.ReliableCollections
{
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;
    using Microsoft.ServiceFabric.Data.Notifications;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Implements IReliableDictionary.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class MockReliableDictionary<TKey, TValue> : TransactedConcurrentDictionary<TKey, TValue>, IReliableDictionary2<TKey, TValue>
        where TKey : IEquatable<TKey>, IComparable<TKey>
    {
        private readonly IStateSerializer<TKey> _keySerializer;
        private readonly IStateSerializer<TValue> _valueSerializer;

        public long Count => Dictionary.Count;

        public MockReliableDictionary(Uri uri)
            : base(uri)
        {
            // Set the OnDictionaryChanged callback to fire the DictionaryChanged event.
            InternalDictionaryChanged +=
                (sender, c) =>
                {
                    if (DictionaryChanged != null)
                    {
                        NotifyDictionaryChangedEventArgs<TKey, TValue> e = null;
                        switch (c.ChangeType)
                        {
                            case ChangeType.Added:
                                e = new NotifyDictionaryItemAddedEventArgs<TKey, TValue>(c.Transaction, c.Key, c.Added);
                                break;
                            case ChangeType.Removed:
                                e = new NotifyDictionaryItemRemovedEventArgs<TKey, TValue>(c.Transaction, c.Key);
                                break;
                            case ChangeType.Updated:
                                e = new NotifyDictionaryItemUpdatedEventArgs<TKey, TValue>(c.Transaction, c.Key, c.Added);
                                break;
                            case ChangeType.Cleared:
                                e = new NotifyDictionaryClearEventArgs<TKey, TValue>(c.Transaction.TransactionId);
                                break;
                        }

                        DictionaryChanged.Invoke(this, e);
                    }

                    MockDictionaryChanged?.Invoke(this, c);
                };
        }

        internal MockReliableDictionary(Uri uri, IReadOnlyStateSerializerStore serializerStore)
            : this(uri)
        {
            serializerStore.TryGetStateSerializer(out IStateSerializer<TKey> keySerializer);
            _keySerializer = keySerializer;

            serializerStore.TryGetStateSerializer(out IStateSerializer<TValue> valueSerializer);
            _valueSerializer = valueSerializer;
        }

        /// <summary>
        /// Clones <paramref name="key"/> if there is a key serializer registered for it. If not, returns the same value.
        /// Used to ensure that when the value is serializable, no direct reference to an object enters or leaves the reliable collection.
        /// </summary>
        /// <param name="key"></param>
        private TKey MaybeCloneKey(TKey key)
        {
            if (!key.Equals(default) && _keySerializer != null)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (BinaryWriter binaryWriter = new BinaryWriter(memoryStream))
                    using (BinaryReader binaryReader = new BinaryReader(memoryStream))
                    {
                        _keySerializer.Write(key, binaryWriter);
                        // Set the position back to the beginning of the stream before reading.
                        memoryStream.Position = 0;
                        return _keySerializer.Read(binaryReader);
                    }
                }
            }

            return key;
        }

        /// <summary>
        /// Clones <paramref name="value"/> if there is a value serializer registered for it. If not, returns the same value.
        /// Used to ensure that when the value is serializable, no direct reference to an object enters or leaves the reliable collection.
        /// </summary>
        /// <param name="value"></param>
        private TValue MaybeCloneValue(TValue value)
        {
            if (!value.Equals(default) && _valueSerializer != null)
            {
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    using (BinaryWriter binaryWriter = new BinaryWriter(memoryStream))
                    using (BinaryReader binaryReader = new BinaryReader(memoryStream))
                    {
                        _valueSerializer.Write(value, binaryWriter);
                        // Set the position back to the beginning of the stream before reading.
                        memoryStream.Position = 0;
                        return _valueSerializer.Read(binaryReader);
                    }
                }
            }

            return value;
        }

        /// <summary>
        /// Never invoked, but settable.
        /// </summary>
        public Func<IReliableDictionary<TKey, TValue>, NotifyDictionaryRebuildEventArgs<TKey, TValue>, Task> RebuildNotificationAsyncCallback
        {
            get;
            set;
        }



        public event EventHandler<NotifyDictionaryChangedEventArgs<TKey, TValue>> DictionaryChanged;
        public event EventHandler<DictionaryChangedEvent<TKey, TValue>> MockDictionaryChanged;

        #region AddAsync
        public Task AddAsync(ITransaction tx, TKey key, TValue value)
        {
            return base.AddAsync(tx, MaybeCloneKey(key), MaybeCloneValue(value));
        }
        #endregion

        #region AddOrUpdateAsync
        public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return base.AddOrUpdateAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(addValueFactory(k)), (k, v) => MaybeCloneValue(updateValueFactory(k, v)));
        }

        public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return base.AddOrUpdateAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(addValue), (k, v) => MaybeCloneValue(updateValueFactory(k, v)));
        }

        public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return base.AddOrUpdateAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(addValue), (k, v) => MaybeCloneValue(updateValueFactory(k, v)), timeout, cancellationToken);
        }
        #endregion

        #region ClearAsync
        public Task ClearAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return base.ClearAsync();
        }
        #endregion

        #region ContainsKeyAsync
        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key)
        {
            return base.ContainsKeyAsync(tx, key, LockMode.Default);
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode)
        {
            return base.ContainsKeyAsync(tx, key, lockMode);
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return base.ContainsKeyAsync(tx, key, LockMode.Default, timeout, cancellationToken);
        }
        #endregion

        #region CreateEnumerableAsync
        public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx)
        {
            return CreateEnumerableAsync(tx, EnumerationMode.Unordered);
        }

        public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
        {
            return CreateEnumerableAsync(tx, null, enumerationMode);
        }

        public async Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx, Func<TKey, bool> filter, EnumerationMode enumerationMode)
        {
            List<TKey> keys = new List<TKey>();

            try
            {
                BeginTransaction(tx);

                foreach (var key in Dictionary.Keys)
                {
                    if (filter == null || filter(key))
                    {
                        await LockManager.AcquireLock(tx.TransactionId, key, LockMode.Default);
                        keys.Add(key);
                    }
                }

                if (enumerationMode == EnumerationMode.Ordered)
                {
                    keys.Sort();
                }

                IAsyncEnumerable<KeyValuePair<TKey, TValue>> result = new MockAsyncEnumerable<KeyValuePair<TKey, TValue>>(keys.Select(k => new KeyValuePair<TKey, TValue>(MaybeCloneKey(k), MaybeCloneValue(Dictionary[k]))));
                return result;
            }
            catch
            {
                foreach (var key in keys)
                {
                    LockManager.ReleaseLock(tx.TransactionId, key);
                }

                throw;
            }
        }

        #endregion

        #region CreateKeyEnumerableAsync
        public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx)
        {
            return CreateKeyEnumerableAsync(tx, EnumerationMode.Unordered, default(TimeSpan), CancellationToken.None);
        }

        public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
        {
            return CreateKeyEnumerableAsync(tx, enumerationMode, default(TimeSpan), CancellationToken.None);
        }

        public async Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            List<TKey> keys = new List<TKey>();

            try
            {
                BeginTransaction(tx);
                foreach (var key in Dictionary.Keys)
                {
                    await LockManager.AcquireLock(tx.TransactionId, key, LockMode.Default, timeout, cancellationToken);
                    keys.Add(key);
                }

                if (enumerationMode == EnumerationMode.Ordered)
                {
                    keys.Sort();
                }

                IAsyncEnumerable<TKey> result = new MockAsyncEnumerable<TKey>(keys.Select((key) => MaybeCloneKey(key)));
                return result;
            }
            catch
            {
                foreach (var key in keys)
                {
                    LockManager.ReleaseLock(tx.TransactionId, key);
                }

                throw;
            }
        }
        #endregion

        #region GetOrAddAsync
        public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory)
        {
            return MaybeCloneValue(await base.GetOrAddAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(valueFactory(k))));
        }

        public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value)
        {
            return MaybeCloneValue(await base.GetOrAddAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(value)));
        }

        public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return MaybeCloneValue(await base.GetOrAddAsync(tx, MaybeCloneKey(key), (k) => MaybeCloneValue(value), timeout, cancellationToken));
        }
        #endregion

        #region SetAsync
        public Task SetAsync(ITransaction tx, TKey key, TValue value)
        {
            return base.SetAsync(tx, MaybeCloneKey(key), MaybeCloneValue(value), default(TimeSpan), CancellationToken.None);
        }
        #endregion

        #region TryAddValue
        public Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value)
        {
            return base.TryAddAsync(tx, MaybeCloneKey(key), MaybeCloneValue(value), default(TimeSpan), CancellationToken.None);
        }
        #endregion

        #region TryGetValueAsync
        public async Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key)
        {
            var conditionalValue = await base.TryGetValueAsync(tx, key, LockMode.Default, default(TimeSpan), CancellationToken.None);
            return new ConditionalValue<TValue>(conditionalValue.HasValue, MaybeCloneValue(conditionalValue.Value));
        }

        public async Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode)
        {
            var conditionalValue = await base.TryGetValueAsync(tx, key, lockMode, default(TimeSpan), CancellationToken.None);
            return new ConditionalValue<TValue>(conditionalValue.HasValue, MaybeCloneValue(conditionalValue.Value));
        }

        public async Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var conditionalValue = await base.TryGetValueAsync(tx, key, LockMode.Default, timeout, cancellationToken);
            return new ConditionalValue<TValue>(conditionalValue.HasValue, MaybeCloneValue(conditionalValue.Value));
        }
        #endregion

        #region TryRemoveValueAsync
        public async Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key)
        {
            var conditionalValue = await base.TryRemoveAsync(tx, key, default(TimeSpan), CancellationToken.None);
            return new ConditionalValue<TValue>(conditionalValue.HasValue, MaybeCloneValue(conditionalValue.Value));
        }
        #endregion

        #region TryUpdateAsync
        public Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue)
        {
            return base.TryUpdateAsync(tx, key, MaybeCloneValue(newValue), comparisonValue, default(TimeSpan), CancellationToken.None);
        }
        #endregion
    }
}
