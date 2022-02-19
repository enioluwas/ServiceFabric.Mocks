using Microsoft.ServiceFabric.Data;
using System;
using System.IO;

namespace ServiceFabric.Mocks.NetCoreTests.MocksTests
{
    internal class TestUserKey : IEquatable<TestUserKey>, IComparable<TestUserKey>
    {
        public Guid Key { get; set; }

        public int CompareTo(TestUserKey other)
        {
            return Key.CompareTo(other.Key);
        }

        public bool Equals(TestUserKey other)
        {
            return other != null && Key == other.Key;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TestUserKey);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Key);
        }
    }

    internal class TestUserKeyStateSerializer : IStateSerializer<TestUserKey>
    {
        public TestUserKey Read(BinaryReader binaryReader)
        {
            var key = binaryReader.ReadString();
            return new TestUserKey { Key = Guid.Parse(key) };
        }

        public TestUserKey Read(TestUserKey baseValue, BinaryReader binaryReader)
        {
            return Read(binaryReader);
        }

        public void Write(TestUserKey value, BinaryWriter binaryWriter)
        {
            binaryWriter.Write(value.Key.ToString());
        }

        public void Write(TestUserKey baseValue, TestUserKey targetValue, BinaryWriter binaryWriter)
        {
            Write(targetValue, binaryWriter);
        }
    }
}
