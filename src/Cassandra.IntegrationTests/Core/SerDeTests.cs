using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading;
using Cassandra.Tests;
using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class SerDeTests : SharedClusterTest
    {
        public SerDeTests() : base(3) { }

        // Track tables and UDT types created by each test so we can drop them in TearDown
        private List<string> _tablesToDrop;
        private List<string> _udtTypesToDrop;

        [SetUp]
        public void TestSetup()
        {
            _tablesToDrop = new List<string>();
            _udtTypesToDrop = new List<string>();
        }

        [TearDown]
        public void TestTearDown()
        {
            // Drop tables first
            if (_tablesToDrop != null)
            {
                foreach (var table in _tablesToDrop)
                {
                    try
                    {
                        Session.Execute($"DROP TABLE IF EXISTS {table}");
                    }
                    catch (Exception)
                    {
                        // Ignore errors during cleanup
                    }
                }
            }

            // Drop UDT types
            if (_udtTypesToDrop != null)
            {
                foreach (var udt in _udtTypesToDrop)
                {
                    try
                    {
                        Session.Execute($"DROP TYPE IF EXISTS {KeyspaceName}.{udt}");
                    }
                    catch (Exception)
                    {
                        // Ignore errors during cleanup
                    }
                }
            }
        }

        public static IEnumerable<TestCaseData> GetSerializationTestData()
        {
            var data = new List<TestCaseData>
            {
                new TestCaseData("ascii", "ascii text"),
                new TestCaseData("bigint", 1234567890123L),
                new TestCaseData("blob", new byte[] { 1, 2, 3, 4, 5 }),
                new TestCaseData("boolean", true),
                new TestCaseData("date", new LocalDate(2026, 1, 8)),
                new TestCaseData("decimal", 123.456M),
                new TestCaseData("double", 1234.5678D),
                new TestCaseData("duration", new Duration(1, 2, 3)),
                new TestCaseData("float", 12.34F),
                new TestCaseData("inet", IPAddress.Parse("127.0.0.1")),
                new TestCaseData("int", 12345),
                new TestCaseData("smallint", (short)123),
                new TestCaseData("text", "some text"),
                new TestCaseData("time", new LocalTime(12, 34, 56, 789)),
                new TestCaseData("timestamp", DateTimeOffset.FromUnixTimeMilliseconds(1697371200000)),
                new TestCaseData("timeuuid", Guid.Parse("50554d6e-29bb-11e5-b345-feff819cdc9f")),
                new TestCaseData("tinyint", (sbyte)12),
                new TestCaseData("uuid", Guid.Parse("50554d6e-29bb-11e5-b345-feff819cdc9f")),
                new TestCaseData("varchar", "varchar value"),
                new TestCaseData("varint", BigInteger.Parse("123456789012345678901234567890")),
                new TestCaseData("list<int>", new List<int> { 1, 2, 3 }),
                new TestCaseData("list<text>", new List<string> { "one", "two" }),
                new TestCaseData("set<int>", new List<int> { 1, 2, 3 }),
                new TestCaseData("map<text, int>", new Dictionary<string, int> { { "key1", 1 }, { "key2", 2 } }),
                new TestCaseData("tuple<int, text>", Tuple.Create(1, "one")),
                // Complex/Nested types
                new TestCaseData("list<uuid>", new List<Guid> { Guid.NewGuid(), Guid.NewGuid() }),
                new TestCaseData("set<double>", new List<double> { 1.1, 2.2, 3.3 }),
                new TestCaseData("map<int, text>", new Dictionary<int, string> { { 1, "one" }, { 2, "two" } }),
                new TestCaseData("tuple<text, int, uuid>", Tuple.Create("item", 100, Guid.NewGuid())),
                new TestCaseData("list<tuple<int, int>>", new List<Tuple<int, int>> { Tuple.Create(1, 2), Tuple.Create(3, 4) }),
                new TestCaseData("map<text, frozen<list<int>>>", new Dictionary<string, List<int>> { { "a", new List<int> { 1, 2 } }, { "b", new List<int> { 3 } } }),
                new TestCaseData("tuple<list<int>, map<text, int>>", Tuple.Create(new List<int>{1, 2}, new Dictionary<string, int>{{"a", 1}}))
            };
            return data;
        }

        public static IEnumerable<TestCaseData> GetEdgeCaseSerializationTestData()
        {
            var data = new List<TestCaseData>
            {
                 // Nulls
                 new TestCaseData("text", null),
                 new TestCaseData("int", null),
                 new TestCaseData("uuid", null),

                 // Empty Strings
                 new TestCaseData("text", ""),

                 // Min/Max Primitives
                 new TestCaseData("int", int.MaxValue),
                 new TestCaseData("int", int.MinValue),
                 new TestCaseData("bigint", long.MaxValue),
                 new TestCaseData("bigint", long.MinValue),
                 new TestCaseData("double", double.MaxValue),
                 new TestCaseData("double", double.MinValue),
                 new TestCaseData("float", float.MaxValue),
                 new TestCaseData("float", float.MinValue),
                 new TestCaseData("smallint", short.MaxValue),
                 new TestCaseData("smallint", short.MinValue),
                 new TestCaseData("tinyint", sbyte.MaxValue),
                 new TestCaseData("tinyint", sbyte.MinValue),

                 // Empty Collections
                 new TestCaseData("list<int>", new List<int>()),
                 new TestCaseData("set<text>", new List<string>()),
                 new TestCaseData("map<text, int>", new Dictionary<string, int>()),
            };
            return data;
        }

        [Test]
        [TestCaseSource(nameof(GetSerializationTestData))]
        [TestCaseSource(nameof(GetEdgeCaseSerializationTestData))]
        public void ValuesSerializeAndDeserializeCorrectly_Parametrized_Test(string cqlType, object value)
        {
            var safeTypeName = cqlType.Replace("<", "_").Replace(">", "_").Replace(",", "_").Replace(" ", "");
            var tableName = $"{KeyspaceName}.serialization_{safeTypeName}";

            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (id uuid PRIMARY KEY, val {cqlType})");

            // Register table for cleanup
            _tablesToDrop.Add(tableName);

            // TODO: remove after schema agreement bug is fixed
            Thread.Sleep(1000);

            var id = Guid.NewGuid();
            Session.Execute(new SimpleStatement($"INSERT INTO {tableName} (id, val) VALUES (?, ?)", id, value));

            var row = Session.Execute(new SimpleStatement($"SELECT val FROM {tableName} WHERE id = ?", id)).FirstOrDefault();
            Assert.NotNull(row);
            
            var expectedType = value?.GetType() ?? typeof(object);
            var retrievedValue = row!.GetValue(expectedType, "val");
            VerifyValuesAreEqual(value, retrievedValue);
        }

        private void VerifyValuesAreEqual(object expected, object actual)
        {
            if (expected == null && actual == null)
            {
                return;
            }

            // Treat empty collection as null for comparison (Cassandra behavior)
            if (actual == null && expected is ICollection { Count: 0 })
            {
                return;
            }

            Assert.NotNull(expected, $"Expected value is null but actual is {actual}");
            Assert.NotNull(actual, $"Actual value is null but expected is {expected}");

            if (expected!.GetType().Name.StartsWith("Tuple"))
            {
                VerifyTuplesAreEqual((System.Runtime.CompilerServices.ITuple)expected, (System.Runtime.CompilerServices.ITuple)actual);
            }
            else if (expected is IDictionary expDict && actual is IDictionary actDict)
            {
                VerifyDictionariesAreEqual(expDict, actDict);
            }
            else if ((expected is IEnumerable expEnum) && (expected is not string) &&
                     (actual is IEnumerable actEnum) && (actual is not string))
            {
                VerifyCollectionsAreEqual(expEnum, actEnum);
            }
            else
            {
                Assert.AreEqual(expected, actual);
            }
        }

        private void VerifyTuplesAreEqual(System.Runtime.CompilerServices.ITuple expected, System.Runtime.CompilerServices.ITuple actual)
        {
            Assert.AreEqual(expected.Length, actual.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                VerifyValuesAreEqual(expected[i], actual[i]);
            }
        }

        private void VerifyDictionariesAreEqual(IDictionary expected, IDictionary actual)
        {
            Assert.AreEqual(expected.Count, actual.Count);
            foreach (var key in expected.Keys)
            {
                Assert.True(actual.Contains(key), $"Dictionary missing key: {key}");
                VerifyValuesAreEqual(expected[key], actual[key]);
            }
        }

        private void VerifyCollectionsAreEqual(IEnumerable expected, IEnumerable actual)
        {
            var expList = expected.Cast<object>().ToList();
            var actList = actual.Cast<object>().ToList();

            Assert.AreEqual(expList.Count, actList.Count);
            for (int i = 0; i < expList.Count; i++)
            {
                 VerifyValuesAreEqual(expList[i], actList[i]);
            }
        }
        [Test]
        public void SimpleStatement_InsertAllPrimitiveTypes()
        {
            var tableName = $"{KeyspaceName}.AllPrimitivesTable";
            Session.Execute($@"
                CREATE TABLE IF NOT EXISTS {tableName} (
                    id uuid PRIMARY KEY,
                    col_ascii ascii,
                    col_bigint bigint,
                    col_blob blob,
                    col_boolean boolean,
                    col_date date,
                    col_decimal decimal,
                    col_double double,
                    col_duration duration,
                    col_float float,
                    col_inet inet,
                    col_int int,
                    col_smallint smallint,
                    col_text text,
                    col_time time,
                    col_timestamp timestamp,
                    col_timeuuid timeuuid,
                    col_tinyint tinyint,
                    col_uuid uuid,
                    col_varchar varchar,
                    col_varint varint
                )");

            // Register table for cleanup
            _tablesToDrop.Add(tableName);

            // TODO: remove after schema agreement bug is fixed
            Thread.Sleep(1000);

            var id = Guid.NewGuid();
            var asciiVal = "ascii text";
            var bigintVal = 1234567890123L;
            var blobVal = new byte[] { 1, 2, 3, 4, 5 };
            var booleanVal = true;
            var dateVal = new LocalDate(2023, 10, 15);
            var decimalVal = 123.456M;
            var doubleVal = 1234.5678D;
            var durationVal = new Duration(1, 2, 3);
            var floatVal = 12.34F;
            var inetVal = IPAddress.Parse("127.0.0.1");
            var intVal = 12345;
            var smallintVal = (short)123;
            var textVal = "some text 😀";
            var timeVal = new LocalTime(12, 34, 56, 789);
            var timestampVal = DateTimeOffset.UtcNow;
            var timeUuidVal = TimeUuid.NewId();
            var tinyintVal = (sbyte)12;
            var uuidVal = Guid.NewGuid();
            var varcharVal = "varchar value";
            var varintVal = BigInteger.Parse("123456789012345678901234567890");

            var insertQuery = new SimpleStatement($@"
                INSERT INTO {tableName} (
                    id, col_ascii, col_bigint, col_blob, col_boolean, col_date,
                    col_decimal, col_double, col_duration, col_float, col_inet,
                    col_int, col_smallint, col_text, col_time, col_timestamp,
                    col_timeuuid, col_tinyint, col_uuid, col_varchar, col_varint
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )",
                // pack values into an object array to avoid analyzer false positives on long argument lists
                new object[] { id, asciiVal, bigintVal, blobVal, booleanVal, dateVal,
                    decimalVal, doubleVal, durationVal, floatVal, inetVal,
                    intVal, smallintVal, textVal, timeVal, timestampVal,
                    timeUuidVal, tinyintVal, uuidVal, varcharVal, varintVal }
             );

            Session.Execute(insertQuery);

            var row = Session.Execute(new SimpleStatement($"SELECT * FROM {tableName} WHERE id = ?", id)).FirstOrDefault();
            Assert.NotNull(row);

            Assert.AreEqual(asciiVal, row!.GetValue<string>("col_ascii"));
            Assert.AreEqual(bigintVal, row.GetValue<long>("col_bigint"));
            Assert.AreEqual(blobVal, row.GetValue<byte[]>("col_blob"));
            Assert.AreEqual(booleanVal, row.GetValue<bool>("col_boolean"));
            Assert.AreEqual(dateVal, row.GetValue<LocalDate>("col_date"));
            Assert.AreEqual(decimalVal, row.GetValue<decimal>("col_decimal"));
            Assert.AreEqual(doubleVal, row.GetValue<double>("col_double"));
            Assert.AreEqual(durationVal, row.GetValue<Duration>("col_duration"));
            Assert.AreEqual(floatVal, row.GetValue<float>("col_float"));
            Assert.AreEqual(inetVal, row.GetValue<IPAddress>("col_inet"));
            Assert.AreEqual(intVal, row.GetValue<int>("col_int"));
            Assert.AreEqual(smallintVal, row.GetValue<short>("col_smallint"));
            Assert.AreEqual(textVal, row.GetValue<string>("col_text"));
            Assert.AreEqual(timeVal, row.GetValue<LocalTime>("col_time"));
            Assert.AreEqual(timestampVal.ToUnixTimeMilliseconds(), row.GetValue<DateTimeOffset>("col_timestamp").ToUnixTimeMilliseconds());
            Assert.AreEqual(timeUuidVal.ToGuid(), row.GetValue<Guid>("col_timeuuid"));
            Assert.AreEqual(tinyintVal, row.GetValue<sbyte>("col_tinyint"));
            Assert.AreEqual(uuidVal, row.GetValue<Guid>("col_uuid"));
            Assert.AreEqual(varcharVal, row.GetValue<string>("col_varchar"));
            Assert.AreEqual(varintVal, row.GetValue<BigInteger>("col_varint"));
        }

        [Test]
        public void SimpleStatement_Udt()
        {
            const string typeName = "address_type";
            var tableName = $"{KeyspaceName}.udt_table";

            Assert.NotNull(Session, "Session should not be null");
            Assert.NotNull(Session.UserDefinedTypes, "Session.UserDefinedTypes should not be null");

            // Define the mapping early
            Session.UserDefinedTypes.Define(
                UdtMap.For<TestAddress>(typeName, KeyspaceName)
                    .Map(a => a.Street, "street")
                    .Map(a => a.Number, "number")
            );

            Session.Execute($"CREATE TYPE IF NOT EXISTS {KeyspaceName}.{typeName} (street text, number int)");

            // Register udt type for cleanup
            _udtTypesToDrop.Add(typeName);

            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (id uuid PRIMARY KEY, addr frozen<{typeName}>)");

            // Register table for cleanup
            _tablesToDrop.Add(tableName);

            // TODO: remove after schema agreement bug is fixed
            Thread.Sleep(2000);

            var id = Guid.NewGuid();
            var address = new TestAddress { Street = "Main St", Number = 123 };

            // Use Prepared Statements for UDTs to ensure correct metadata handling and serialization
            var psInsert = Session.Prepare($"INSERT INTO {tableName} (id, addr) VALUES (?, ?)");
            Session.Execute(psInsert.Bind(id, address));

            var psSelect = Session.Prepare($"SELECT addr FROM {tableName} WHERE id = ?");
            var row = Session.Execute(psSelect.Bind(id)).FirstOrDefault();
            
            Assert.NotNull(row, "Row was null, which usually means the INSERT failed or schema was not ready");

            var retrievedAddress = row!.GetValue<TestAddress>("addr");
            Assert.NotNull(retrievedAddress);
            Assert.AreEqual(address.Street, retrievedAddress.Street);
            Assert.AreEqual(address.Number, retrievedAddress.Number);
        }

        [Test]
        [Ignore("Vector type deserialization not yet implemented")]
        public void SimpleStatement_Vector()
        {
            // Vector support requires newer Cassandra/Scylla versions.
            // We check if the type is supported by trying to create a table.
            var tableName = $"{KeyspaceName}.vector_table";
            try
            {
                Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (id uuid PRIMARY KEY, vec vector<float, 3>)");

                // Register table for cleanup
                _tablesToDrop.Add(tableName);
            }
            catch (InvalidQueryException)
            {
                NUnit.Framework.Assert.Ignore("Vector type not supported by the current cluster version.");
                return;
            }

            // TODO: remove after schema agreement bug is fixed
            Thread.Sleep(1000);

            var id = Guid.NewGuid();
            var vector = new CqlVector<float>(new[] { 1.1f, 2.2f, 3.3f });

            Session.Execute(new SimpleStatement($"INSERT INTO {tableName} (id, vec) VALUES (?, ?)", id, vector));

            var row = Session.Execute(new SimpleStatement($"SELECT vec FROM {tableName} WHERE id = ?", id)).FirstOrDefault();
            Assert.NotNull(row);

            var retrievedVector = row!.GetValue<CqlVector<float>>("vec");
            Assert.NotNull(retrievedVector);
            Assert.AreEqual(vector.Count, retrievedVector.Count);
            Assert.AreEqual(vector[0], retrievedVector[0]);
            Assert.AreEqual(vector[1], retrievedVector[1]);
            Assert.AreEqual(vector[2], retrievedVector[2]);
        }
    }
}

