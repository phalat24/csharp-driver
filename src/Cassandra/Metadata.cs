//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    ///  Keeps metadata on the connected cluster, including known nodes and schema
    ///  definitions.
    /// </summary>
    public class Metadata : IDisposable
    {
#pragma warning disable CS0067
        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;
#pragma warning restore CS0067
        /// <summary>
        ///  Returns the name of currently connected cluster.
        /// </summary>
        /// <returns>the Cassandra name of currently connected cluster.</returns>
        public String ClusterName { get; internal set; }

        /// <summary>
        /// Determines whether the cluster is provided as a service.
        /// </summary>
        public bool IsDbaas { get; private set; } = false;

        /// <summary>
        /// Gets the configuration associated with this instance.
        /// </summary>
        internal Configuration Configuration { get; private set; }

        // Function to get an active session from the cluster for FFI calls.
        // Provided by Cluster during construction. It never returns null.
        // It either returns a valid Session or throws InvalidOperationException.
        private readonly Func<Session> _getActiveSessionOrThrow;

        internal class RefreshContext(IReadOnlyDictionary<Guid, Host> oldHosts)
        {
            private readonly Dictionary<Guid, Host> _newHosts = new Dictionary<Guid, Host>();
            private readonly Dictionary<IPEndPoint, Guid> _newHostIdsByIp = new Dictionary<IPEndPoint, Guid>();

            internal IReadOnlyDictionary<Guid, Host> OldHosts { get; } = oldHosts;

            internal void AddHost(Host host)
            {
                _newHosts[host.HostId] = host;
                _newHostIdsByIp[host.Address] = host.HostId;
            }

            internal HostRegistry ToNewRegistry() => new HostRegistry(_newHosts, _newHostIdsByIp);
        }

        // HostRegistry groups both maps so they can be swapped atomically.
        internal sealed class HostRegistry(
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyDictionary<IPEndPoint, Guid> hostIdsByIp)
        {
            internal readonly IReadOnlyDictionary<Guid, Host> HostsById =
                hostsById ?? new Dictionary<Guid, Host>();

            internal readonly IReadOnlyDictionary<IPEndPoint, Guid> HostIdsByIp =
                hostIdsByIp ?? new Dictionary<IPEndPoint, Guid>();
        }

        // ClusterSnapshot couples a BridgedClusterState with the HostRegistry built from it.
        // Each instance owns a reference count on the underlying BridgedClusterState (SafeHandle),
        // analogous to Arc in Rust. Construction increments the refcount; Dispose decrements it.
        // The native resource is freed only when the last refcount drops.
        private sealed class ClusterSnapshot : IDisposable
        {
            internal BridgedClusterState State { get; }
            internal HostRegistry Registry { get; }

            /// <summary>
            /// Clones an existing snapshot, incrementing the refcount on the underlying state.
            /// Analogous to <c>Arc::clone</c> in Rust.
            /// </summary>
            /// <exception cref="ObjectDisposedException">The source snapshot's state has already been freed.</exception>
            internal static ClusterSnapshot CloneByRef(ClusterSnapshot other)
            {
                if (!other.State.TryIncreaseReferenceCount())
                    throw new ObjectDisposedException(nameof(ClusterSnapshot),
                        "Cannot clone a snapshot whose native state has already been freed.");
                return new ClusterSnapshot(other.State, other.Registry);
            }

            /// <summary>
            /// Takes ownership of an existing refcount on <paramref name="state"/>.
            /// Used when building from a freshly acquired BridgedClusterState.
            /// </summary>
            private ClusterSnapshot(BridgedClusterState state, HostRegistry registry)
            {
                State = state;
                Registry = registry;
            }

            /// <summary>
            /// Builds a new snapshot from a freshly acquired <paramref name="state"/>,
            /// taking ownership of its refcount. Reuses existing Host instances from
            /// <paramref name="oldRegistry"/> where possible.
            /// </summary>
            internal static ClusterSnapshot BuildFromFreshState(
                BridgedClusterState state, HostRegistry oldRegistry)
            {
                RefreshContext context;
                try
                {
                    var hostsById = oldRegistry?.HostsById ?? new Dictionary<Guid, Host>();
                    context = new(hostsById);
                    state.FillHostCache(context);
                }
                catch (Exception)
                {
                    // If FillHostCache throws before ownership is transferred to the snapshot,
                    // dispose eagerly here rather than relying on SafeHandle finalization.
                    state.DecreaseReferenceCount();
                    throw;
                }
                return new ClusterSnapshot(state, context.ToNewRegistry());
            }

            public void Dispose() => State.DecreaseReferenceCount();
        }

        private volatile ClusterSnapshot _cachedSnapshot = null;

        private readonly object _hostLock = new object();

        internal Metadata(Configuration configuration, Func<Session> getActiveSessionOrThrow)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
        }

        public void Dispose()
        {
            lock (_hostLock)
            {
                var old = Interlocked.Exchange(ref _cachedSnapshot, null);
                old?.Dispose();
            }
        }

        public Host GetHost(IPEndPoint address)
        {
            using var snapshot = GetSnapshot();
            return !snapshot.Registry.HostIdsByIp.TryGetValue(address, out var hostId)
                ? null
                : snapshot.Registry.HostsById.GetValueOrDefault(hostId);
        }

        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
            using var snapshot = GetSnapshot();
            return snapshot.Registry.HostIdsByIp.TryGetValue(address, out var hostId)
                ? hostId
                : null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            using var snapshot = GetSnapshot();
            return new List<Host>(snapshot.Registry.HostsById.Values);
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns a <see cref="ClusterSnapshot"/> that owns a refcount on the underlying native state.
        /// The caller MUST dispose the returned snapshot when done (e.g. via <c>using</c>).
        /// </summary>
        private ClusterSnapshot GetSnapshot()
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                // Fast path: lock-free read.
                // Probe the current cluster state and compare against the cached snapshot.
                using (var probeState = session.GetClusterState())
                {
                    var cached = _cachedSnapshot;
                    if (cached != null)
                    {
                        try
                        {
                            // Clone the snapshot (increments refcount). If the state was
                            // already disposed by a concurrent replacement, this throws
                            // and we fall through to the slow path.
                            var borrowed = ClusterSnapshot.CloneByRef(cached);
                            if (borrowed.State.Equals(probeState))
                                return borrowed;

                            // Not a match — release the clone.
                            borrowed.Dispose();
                        }
                        catch (ObjectDisposedException) { }
                    }
                }

                // Slow path: cluster state changed (or no cache exists). Take the lock and rebuild.
                lock (_hostLock)
                {
                    var freshState = session.GetClusterState();
                    var cached = _cachedSnapshot;

                    // Double-check: another thread may have already updated the cache.
                    if (cached != null && cached.State.Equals(freshState))
                    {
                        freshState.Dispose();
                        return ClusterSnapshot.CloneByRef(cached); // Clone for the caller.
                    }

                    var newSnapshot = ClusterSnapshot.BuildFromFreshState(
                        freshState, cached?.Registry);

                    var old = Interlocked.Exchange(ref _cachedSnapshot, newSnapshot);
                    old?.Dispose(); // Release the cache's old refcount.

                    return ClusterSnapshot.CloneByRef(newSnapshot); // Clone for the caller.
                }
            }
            finally
            {
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        /// When the caller doesn't specify a keyspace (either by passing `null` or using
        /// the overload that omits the keyspace), we send this sentinel value to
        /// the Rust bridge. The native replica locator treats the empty string as a
        /// signal to fall back to a SimpleStrategy replication factor of 1.
        /// </summary>
        private const string NoSpecifiedKeyspace = "";

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            ArgumentNullException.ThrowIfNull(partitionKey);

            using var snapshot = GetSnapshot();

            // NOTE: C# Metadata.GetReplicas doesn't provide the table name.
            // For correctness, token computation should use the cluster/table partitioner; and for Scylla
            // tablet routing we also need table context. Until we extend the API/bridge, force Murmur3.
            // FIXME: Use metadata-derived partitioner

            // Coalesce null keyspace to sentinel so the Rust side falls back to
            // SimpleStrategy RF=1, returning only the primary replica.
            return snapshot.State.GetReplicasLegacyMurmur3(
                keyspaceName ?? NoSpecifiedKeyspace, snapshot.Registry.HostsById, partitionKey);
        }

        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            // TODO: is it even correct?
            // The idea is to retrieve the primary replicas for the partition key when the keyspace is not specified,
            // since no replication strategy can be applied - that's how it worked in the original driver.
            // In this case, when no keyspace is specified, the Rust side replica locator with fall back to the default
            // Simple Strategy with RF = 1, which achieves exactly what we're aiming for.
            return GetReplicas(NoSpecifiedKeyspace, partitionKey);
        }

        /// <summary>
        ///  Returns metadata of specified keyspace.
        /// </summary>
        /// <param name="keyspace"> the name of the keyspace for which metadata should be
        ///  returned. </param>
        /// <returns>the metadata of the requested keyspace or <c>null</c> if
        ///  <c>* keyspace</c> is not a known keyspace.</returns>
        public KeyspaceMetadata GetKeyspace(string keyspace)
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                var clusterState = session.GetClusterState();
                return clusterState.GetKeyspaceMetadata(keyspace);
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                using (var clusterState = session.GetClusterState())
                {
                    return clusterState.GetKeyspaceNames();
                }
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        ///  Returns names of all tables which are defined within specified keyspace.
        /// </summary>
        /// <param name="keyspace">the name of the keyspace for which all tables metadata should be
        ///  returned.</param>
        /// <returns>an ICollection of the metadata for the tables defined in this
        ///  keyspace.</returns>
        public ICollection<string> GetTables(string keyspace)
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                using (var clusterState = session.GetClusterState())
                {
                    return clusterState.GetTableNames(keyspace);
                }
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                using (var clusterState = session.GetClusterState())
                {
                    return clusterState.GetTableMetadata(keyspace, tableName);
                }
            }
            finally
            {
                // Release the lock on the session created by calling _getActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        ///  Returns the view metadata for the provided view name in the keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified view is defined.</param>
        /// <param name="name">name of view.</param>
        /// <returns>a MaterializedViewMetadata for the view in the specified keyspace.</returns>
        public MaterializedViewMetadata GetMaterializedView(string keyspace, string name)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public UdtColumnInfo GetUdtDefinition(string keyspace, string typeName)
        {
            var session = _getActiveSessionOrThrow();
            try
            {
                using (var clusterState = session.GetClusterState())
                {
                    return clusterState.GetUdtMetadata(keyspace, typeName);
                }
            }
            finally
            {
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            return Task.FromResult(GetUdtDefinition(keyspace, typeName));
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Function from Cassandra
        /// </summary>
        /// <returns>The function metadata or null if not found.</returns>
        public FunctionMetadata GetFunction(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a aggregate from Cassandra
        /// </summary>
        /// <returns>The aggregate metadata or null if not found.</returns>
        public AggregateMetadata GetAggregate(string keyspace, string name, string[] signature)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the keyspace and token information
        /// </summary>
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            throw new NotImplementedException();
        }

        public void ShutDown(int timeoutMs = Timeout.Infinite)
        {
            // No-op for now - metadata shutdown not yet implemented
            // throw new NotImplementedException();
        }

        public Task Init()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Initiates a schema agreement check.
        /// <para/>
        /// Schema changes need to be propagated to all nodes in the cluster.
        /// Once they have settled on a common version, we say that they are in agreement.
        /// <para/>
        /// This method does not perform retries so
        /// <see cref="ProtocolOptions.MaxSchemaAgreementWaitSeconds"/> does not apply.
        /// </summary>
        /// <returns>True if schema agreement was successful and false if it was not successful.</returns>
        public Task<bool> CheckSchemaAgreementAsync()
        {
            throw new NotImplementedException();
        }
    }
}
