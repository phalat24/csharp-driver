#nullable enable
namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Kept in its own file so multiple test classes can reference it.
    /// </summary>
    public class TestAddress
    {
        public string? Street { get; set; }
        public int Number { get; set; }
    }
}
