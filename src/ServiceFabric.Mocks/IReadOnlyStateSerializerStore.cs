using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.Mocks
{
    internal interface IReadOnlyStateSerializerStore
    {
        bool TryGetStateSerializer<T>(out IStateSerializer<T> stateSerializer);
    }
}
