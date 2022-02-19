using Microsoft.ServiceFabric.Data;
using System;
using System.IO;

namespace ServiceFabric.Mocks.NetCoreTests.MocksTests
{
    internal class TestUser : IEquatable<TestUser>
    {
        public string Name { get; set; }
        public DateTime LastLoginUtc { get; set; }

        public bool Equals(TestUser other)
        {
            return other != null
                && Name == other.Name
                && LastLoginUtc == other.LastLoginUtc;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TestUser);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, LastLoginUtc);
        }
    }

    internal class TestUserStateSerializer : IStateSerializer<TestUser>
    {
        public TestUser Read(BinaryReader binaryReader)
        {
            var name = binaryReader.ReadString();
            var lastLogin = binaryReader.ReadInt64();
            return new TestUser { Name = name, LastLoginUtc = DateTime.FromBinary(lastLogin) };
        }

        public TestUser Read(TestUser baseValue, BinaryReader binaryReader)
        {
            return Read(binaryReader);
        }

        public void Write(TestUser value, BinaryWriter binaryWriter)
        {
            binaryWriter.Write(value.Name);
            binaryWriter.Write(value.LastLoginUtc.ToBinary());
        }

        public void Write(TestUser baseValue, TestUser targetValue, BinaryWriter binaryWriter)
        {
            Write(targetValue, binaryWriter);
        }
    }
}
