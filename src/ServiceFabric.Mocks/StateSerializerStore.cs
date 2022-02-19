using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Concurrent;

namespace ServiceFabric.Mocks
{
    internal class StateSerializerStore : IReadOnlyStateSerializerStore
    {
        private readonly ConcurrentDictionary<Type, object> serializerMap = new ConcurrentDictionary<Type, object>();

        public bool TryAddStateSerializer<T>(IStateSerializer<T> stateSerializer)
        {
            return serializerMap.TryAdd(typeof(T), stateSerializer);
        }

        public bool TryGetStateSerializer<T>(out IStateSerializer<T> stateSerializer)
        {
            var found = serializerMap.TryGetValue(typeof(T), out object rawStateSerializer);
            stateSerializer = (IStateSerializer<T>)rawStateSerializer;
            return found;
        }
    }
}
