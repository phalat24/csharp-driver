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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
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
        private const int HostIdLength = 16;
        public event HostsEventHandler HostsEvent;

        public event SchemaChangedEventHandler SchemaChangedEvent;
#pragma warning restore CS0067

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool cluster_state_compare_ptr(
            IntPtr ptr1,
            IntPtr ptr2);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr cluster_state_get_raw_ptr(IntPtr clusterStatePtr);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FfiException cluster_state_fill_nodes(
            IntPtr clusterStatePtr,
            IntPtr contextPtr,
            IntPtr callback,
            IntPtr constructors);

        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern void cluster_state_free(IntPtr clusterStatePtr);

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, FFIByteSlice, FFIByteSlice, ushort, FFIString, FFIString, RustBridge.FfiException> AddHostPtr = &AddHostToList;

        private static readonly unsafe delegate* unmanaged[Cdecl]<IntPtr, IntPtr, int, void> OnReplicaPairPtr = &OnReplicaPairCallback;

        // NOTE: Token map replica resolution without table context currently forces Murmur3
        // on the Rust side via cluster_state_get_replicas_murmur3.
        [DllImport(NativeLibrary.CSharpWrapper, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustBridge.FfiException cluster_state_get_replicas_murmur3(
            IntPtr clusterStatePtr,
            [MarshalAs(UnmanagedType.LPUTF8Str)] string keyspace,
            [In] byte[] partitionKey,
            nuint partitionKeyLen,
            IntPtr callbackState,
            IntPtr callback,
            IntPtr constructors);

        [UnmanagedCallersOnly(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static unsafe RustBridge.FfiException AddHostToList(
            IntPtr contextPtr,
            FFIByteSlice idBytes,
            FFIByteSlice ipBytes,
            ushort port,
            FFIString datacenter,
            FFIString rack)
        {
            try
            {
                var context = Unsafe.AsRef<RefreshContext>((void*)contextPtr);

                var hostId = new Guid(idBytes.ToSpan());

                var ipAddress = new IPAddress(ipBytes.ToSpan());
                var address = new IPEndPoint(ipAddress, port);

                if (context.OldHosts != null && context.OldHosts.TryGetValue(hostId, out var host))
                {
                    if (host.Address.Equals(address))
                    {
                        context.AddHost(host);
                        return RustBridge.FfiException.Ok();
                    }
                }

                var dcString = datacenter.ToManagedString();
                var rackString = rack.ToManagedString();

                host = new Host(address, hostId, dcString, rackString);
                context.AddHost(host);
                return RustBridge.FfiException.Ok();
            }
            catch (Exception ex)
            {
                return RustBridge.FfiException.FromException(ex);
            }
        }

        private class GetReplicasContext(IReadOnlyDictionary<Guid, Host> hostsById)
        {
            public List<HostShard> Replicas { get; } = [];
            public IReadOnlyDictionary<Guid, Host> HostsById { get; } = hostsById;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void OnReplicaPairCallback(IntPtr statePtr, IntPtr hostIdBytesPtr, int shard)
        {
            try
            {
                // Safety: statePtr points to the stack slot holding the managed GetReplicasContext reference
                // (same pattern as RowSet.SetColumnMeta and Metadata.AddHostToList).
                var context = Unsafe.AsRef<GetReplicasContext>((void*)statePtr);
                if (context == null)
                {
                    Environment.FailFast("Invalid context in OnReplicaPairCallback");
                }

                var hostIdBytes = new ReadOnlySpan<byte>((void*)hostIdBytesPtr, HostIdLength);
                var hostId = new Guid(hostIdBytes);

                if (context.HostsById.TryGetValue(hostId, out var host))
                {
                    context.Replicas.Add(new HostShard(host, shard));
                }
                else
                {
                    // Host not found in metadata, possibly removed or inconsistent state.
                    // We could log this, but for now we just skip it.
                }
            }
            catch (Exception ex)
            {
                Environment.FailFast("Fatal error in OnReplicaPairCallback", ex);
            }
        }

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

        // Pointer to the last cluster state used to detect changes. This is a raw pointer
        // stored only for comparison purposes - it does not extend the lifetime of the ClusterState.
        // Volatile ensures visibility of updates across threads for the lock-free read in AllHosts().
        private volatile IntPtr _lastClusterStatePtr = IntPtr.Zero;

        private class RefreshContext(IReadOnlyDictionary<Guid, Host> oldHosts)
        {
            private readonly Dictionary<Guid, Host> _newHosts = new Dictionary<Guid, Host>();
            private readonly Dictionary<IPEndPoint, Guid> _newHostIdsByIp = new Dictionary<IPEndPoint, Guid>();

            public IReadOnlyDictionary<Guid, Host> OldHosts { get; } = oldHosts;

            public void AddHost(Host host)
            {
                _newHosts[host.HostId] = host;
                _newHostIdsByIp[host.Address] = host.HostId;
            }

            public HostRegistry ToNewRegistry() => new HostRegistry(_newHosts, _newHostIdsByIp);
        }

        // HostRegistry groups both maps so they can be swapped atomically.
        private sealed class HostRegistry(
            IReadOnlyDictionary<Guid, Host> hostsById,
            IReadOnlyDictionary<IPEndPoint, Guid> hostIdsByIp)
        {
            public readonly IReadOnlyDictionary<Guid, Host> HostsById =
                hostsById ?? new Dictionary<Guid, Host>();

            public readonly IReadOnlyDictionary<IPEndPoint, Guid> HostIdsByIp =
                hostIdsByIp ?? new Dictionary<IPEndPoint, Guid>();
        }

        // Active host registry reference; swapped atomically on refresh.
        // NOTE: Do not access this field directly; use GetRegistry() instead, since the accessor covers the
        // refreshment logic, with a compromise between limited data staleness and performance.
        private volatile HostRegistry _hostRegistry =
            new HostRegistry(new Dictionary<Guid, Host>(), new Dictionary<IPEndPoint, Guid>());

        private readonly object _hostLock = new object();

        internal Metadata(Configuration configuration, Func<Session> getActiveSessionOrThrow)
        {
            Configuration = configuration;
            _getActiveSessionOrThrow = getActiveSessionOrThrow ?? throw new ArgumentNullException(nameof(getActiveSessionOrThrow));
        }

        public void Dispose()
        {
            // No-op for now - metadata shutdown not yet implemented
            // throw new NotImplementedException();
        }

        public Host GetHost(IPEndPoint address)
        {
            var registry = GetRegistry();

            return !registry.HostIdsByIp.TryGetValue(address, out var hostId) ? null : registry.HostsById.GetValueOrDefault(hostId);
        }

        internal Guid? GetHostIdByIp(IPEndPoint address)
        {
            if (GetRegistry().HostIdsByIp.TryGetValue(address, out var hostId))
            {
                return hostId;
            }

            return null;
        }

        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        /// <returns>collection of all known hosts of this cluster.</returns>
        public ICollection<Host> AllHosts()
        {
            // Return a snapshot copy of the values as ICollection<Host>
            return new List<Host>(GetRegistry().HostsById.Values);
        }

        public IEnumerable<IPEndPoint> AllReplicas()
        {
            throw new NotImplementedException();
        }
        
        /// <summary>
        /// Returns a registry instance, refreshing topology if needed.
        /// At most one caller performs the refresh (single-flight), others
        /// will use the last known registry.
        /// </summary>
        private HostRegistry GetRegistry()
        {
            var session = _getActiveSessionOrThrow();
            IntPtr clusterStatePtr;

            try
            {
                // Check if cache is valid without lock first using a temporary pointer check.
                // This avoids waiting on the lock for the common case where topology hasn't changed.
                clusterStatePtr = session.GetClusterStatePtr();
                try
                {
                    IntPtr rawPtr = cluster_state_get_raw_ptr(clusterStatePtr);
                    if (_lastClusterStatePtr != IntPtr.Zero && rawPtr == _lastClusterStatePtr)
                    {
                        return _hostRegistry;
                    }
                }
                finally
                {
                    // Free the fetched cluster state pointer.
                    cluster_state_free(clusterStatePtr);
                }

                // Acquire the host lock to perform update if needed.
                lock (_hostLock)
                {
                    // Acquire fresh pointer inside lock - the cluster state may have changed while waiting for lock.
                    clusterStatePtr = session.GetClusterStatePtr();
                    try
                    {
                        // Double-check: another thread may have updated the cache while we waited for lock
                        IntPtr rawPtr = cluster_state_get_raw_ptr(clusterStatePtr);
                        if (_lastClusterStatePtr != IntPtr.Zero && rawPtr == _lastClusterStatePtr)
                        {
                            return _hostRegistry;
                        }

                        // Otherwise we are forced to refill all hosts.
                        RefreshTopologyCacheInternal(clusterStatePtr);

                        // Store the raw pointer address for future comparisons.
                        _lastClusterStatePtr = rawPtr;

                        return _hostRegistry;
                    }
                    finally
                    {
                        // Always free the fetched cluster state pointer to avoid memory leak.
                        cluster_state_free(clusterStatePtr);
                    }
                }
            }
            finally
            {
                // Release the lock on the session created by GetActiveSessionOrThrow.
                session.DecreaseReferenceCount();
            }
        }

        /// <summary>
        /// Updates the cached topology if the cluster state has changed.
        /// </summary>
        private void RefreshTopologyCacheInternal(IntPtr clusterStatePtr)
        {
            var context = new RefreshContext(_hostRegistry.HostsById);
            RustBridge.FfiException res = default;
            try
            {
                unsafe
                {
                    res = cluster_state_fill_nodes(
                        clusterStatePtr,
                        (IntPtr)Unsafe.AsPointer(ref context),
                        (IntPtr)AddHostPtr,
                        (IntPtr)RustBridgeGlobals.ConstructorsPtr
                    );

                    RustBridge.ThrowIfException(ref res);
                }
            }
            finally
            {
                RustBridge.FreeExceptionHandle(ref res);
            }

            GC.KeepAlive(context);

            // Swap both maps together via a new HostRegistry created from the context so readers never see mismatched maps.
            Interlocked.Exchange(ref _hostRegistry, context.ToNewRegistry());
        }


        // for tests
        internal KeyValuePair<string, KeyspaceMetadata>[] KeyspacesSnapshot => throw new NotImplementedException();

        /// <summary>
        /// Get the replicas for a given partition key and keyspace
        /// </summary>
        public ICollection<HostShard> GetReplicas(string keyspaceName, byte[] partitionKey)
        {
            var session = _getActiveSessionOrThrow();
            // FIXME: Handle session disposal race condition similar to AllHosts

            // Request a fresh cluster state pointer for the native replica calculation.
            // We get it once and free it in the finally block below.
            var ptr = session.GetClusterStatePtr();
            try
            {
                // Fetch the latest topology registry for HostId -> Host resolution.
                var hostRegistry = GetRegistry();

                var context = new GetReplicasContext(hostRegistry.HostsById);

                IntPtr callbackPtr;
                unsafe
                {
                    callbackPtr = (IntPtr)OnReplicaPairPtr;
                }

                // Pass a pointer to the stack-local slot that holds the managed 'context' reference.
                // The native function is expected to invoke callbacks synchronously before returning.
                unsafe
                {
                    void* contextPtr = Unsafe.AsPointer(ref context);

                    // NOTE: C# Metadata.GetReplicas doesn't provide the table name.
                    // For correctness, token computation should use the cluster/table partitioner; and for Scylla
                    // tablet routing we also need table context. Until we extend the API/bridge, force Murmur3.
                    // FIXME: Use metadata-derived partitioner
                    RustBridge.FfiException res = default;
                    try
                    {
                        res = cluster_state_get_replicas_murmur3(
                            ptr,
                            keyspaceName,
                            partitionKey,
                            (nuint)(partitionKey?.Length ?? 0),
                            (IntPtr)contextPtr,
                            callbackPtr,
                            (IntPtr)RustBridgeGlobals.ConstructorsPtr
                        );

                        RustBridge.ThrowIfException(ref res);
                    }
                    finally
                    {
                        RustBridge.FreeExceptionHandle(ref res);
                    }
                }

                GC.KeepAlive(context);

                return context.Replicas;
            }
            finally
            {
                cluster_state_free(ptr);
            }
        }

        public ICollection<HostShard> GetReplicas(byte[] partitionKey)
        {
            // TODO: is it even correct?
            // The idea is to retrieve the primary replicas for the partition key when the keyspace is not specified,
            // since no replication strategy can be applied - that's how it worked in the original driver.
            // In this case, when no keyspace is specified, the Rust side replica locator with fall back to the default
            // Simple Strategy with RF = 1, which achieves exactly what we're aiming for.
            return GetReplicas("", partitionKey);
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
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns a collection of all defined keyspaces names.
        /// </summary>
        /// <returns>a collection of all defined keyspaces names.</returns>
        public ICollection<string> GetKeyspaces()
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        /// <summary>
        ///  Returns TableMetadata for specified table in specified keyspace.
        /// </summary>
        /// <param name="keyspace">name of the keyspace within specified table is defined.</param>
        /// <param name="tableName">name of table for which metadata should be returned.</param>
        /// <returns>a TableMetadata for the specified table in the specified keyspace.</returns>
        public TableMetadata GetTable(string keyspace, string tableName)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the definition associated with a User Defined Type from Cassandra
        /// </summary>
        public Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspace, string typeName)
        {
            throw new NotImplementedException();
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
