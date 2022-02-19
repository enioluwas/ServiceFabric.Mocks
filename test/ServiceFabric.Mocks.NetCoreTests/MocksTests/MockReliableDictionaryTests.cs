using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using ServiceFabric.Mocks.ReliableCollections;
using System.Threading;
using Microsoft.ServiceFabric.Data.Collections;

namespace ServiceFabric.Mocks.NetCoreTests.MocksTests
{
    [TestClass]
    public class MockReliableDictionaryTests
    {
        [TestMethod]
        public async Task DictionaryAddDuplicateKeyExceptionTypeTest()
        {
            const string key = "key";
            var dictionary = new MockReliableDictionary<string, string>(new Uri("fabric://MockReliableDictionary"));
            var tx = new MockTransaction(null, 1);

            await dictionary.AddAsync(tx, key, "value");
            await Assert.ThrowsExceptionAsync<ArgumentException>(async () =>
            {
                await dictionary.AddAsync(tx, key, "value");
            });
        }

        [TestMethod]
        public async Task DictionaryAddAndRetrieveTest()
        {
            const string key = "key";
            const string value = "value";

            var dictionary = new MockReliableDictionary<string, string>(new Uri("fabric://MockReliableDictionary"));
            var tx = new MockTransaction(null, 1);

            await dictionary.AddAsync(tx, key, value);
            var actual = await dictionary.TryGetValueAsync(tx, key);

            Assert.AreEqual(actual.Value, value);
        }

        [TestMethod]
        public async Task DictionaryCountTest()
        {
            const string key = "key";
            const string value = "value";

            var dictionary = new MockReliableDictionary<string, string>(new Uri("fabric://MockReliableDictionary"));
            var tx = new MockTransaction(null, 1);

            await dictionary.AddAsync(tx, key, value);
            var actual = dictionary.Count;

            Assert.AreEqual(1, actual);
        }

        [TestMethod]
        public async Task DictionaryCreateKeyEnumerableAsyncTest()
        {
            const string key = "key";
            const string value = "value";

            var dictionary = new MockReliableDictionary<string, string>(new Uri("fabric://MockReliableDictionary"));
            var tx = new MockTransaction(null, 1);

            await dictionary.AddAsync(tx, key, value);
            var enumerable = await dictionary.CreateKeyEnumerableAsync(tx);
            var enumerator = enumerable.GetAsyncEnumerator();
            await enumerator.MoveNextAsync(CancellationToken.None);
            var actual = enumerator.Current;

            Assert.AreEqual(key, actual);
        }

        [TestMethod]
        public async Task ClonesValueWithStateSerializerTest()
        {
            var stateManager = new MockReliableStateManager();
            stateManager.TryAddStateSerializer<TestUser>(new TestUserStateSerializer());
            var userDict = await stateManager.GetOrAddAsync<IReliableDictionary2<TestUserKey, TestUser>>("test");
            var userKey = new TestUserKey { Key = Guid.NewGuid() };
            var user = new TestUser { Name = "Gail", LastLoginUtc = DateTime.UtcNow };

            using (var tx = stateManager.CreateTransaction())
            {
                await userDict.AddAsync(tx, userKey, user);
                await tx.CommitAsync();
            }

            // Update the in-memory user's LastLogin.
            user.LastLoginUtc = DateTime.UtcNow;

            using (var tx = stateManager.CreateTransaction())
            {
                var storedUser = (await userDict.TryGetValueAsync(tx, userKey)).Value;
                await tx.CommitAsync();

                Assert.AreNotSame(user, storedUser);
                Assert.AreEqual(user.Name, storedUser.Name);
                Assert.AreNotEqual(user.LastLoginUtc, storedUser.LastLoginUtc);
            }

            // Modifying reference to the key should disable getting the corresponding value in the dictionary,
            // since it does not update the reference in the dictionary.
            userKey.Key = Guid.NewGuid();

            using (var tx = stateManager.CreateTransaction())
            {
                var storedUser = (await userDict.TryGetValueAsync(tx, userKey));
                await tx.CommitAsync();
                Assert.IsFalse(storedUser.HasValue);
            }
        }
    }
}
