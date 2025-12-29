use crate::CSharpStr;
use crate::FfiPtr;
use crate::error_conversion::FfiException;
use crate::ffi::{
    ArcFFI, BridgedBorrowedSharedPtr, BridgedOwnedSharedPtr, FFI, FFIByteSlice, FFIStr, FromArc,
};
use crate::pre_serialized_values::csharp_memory::{CsharpSerializedValue, CsharpValuePtr};
use crate::pre_serialized_values::pre_serialized_values::PreSerializedValues;
use crate::task::ExceptionConstructors;
use scylla::cluster::{ClusterState, Node};
use scylla::errors::ClusterStateTokenError;
use scylla::routing::partitioner::PartitionerName;
use scylla::routing::{Shard, Token};
use std::ffi::CStr;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::Arc;
use thiserror::Error;

impl FFI for ClusterState {
    type Origin = FromArc;
}

/// Frees a ClusterState pointer obtained from `session_get_cluster_state`.
///
/// # Safety
/// - Must only be called once per pointer
/// - The pointer must have been obtained from `session_get_cluster_state`
/// - After calling this function, the pointer is invalid and must not be used
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_free(cluster_state_ptr: BridgedOwnedSharedPtr<ClusterState>) {
    ArcFFI::free(cluster_state_ptr);
    tracing::trace!("[FFI] ClusterState pointer freed");
}

/// This function returns the raw memory address of the underlying ClusterState object
/// for comparison purposes. The returned address can be stored and compared
/// with addresses from other ClusterState pointers to detect topology changes.
///
/// The returned address is ONLY valid for comparison while the Arc<ClusterState> is alive.
///
/// # Safety
/// - The pointer must be valid and not freed
/// - The returned address must only be used for comparison, never dereferenced
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_raw_ptr(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
) -> *const c_void {
    cluster_state_ptr
        .to_raw()
        .map(|p| p as *const c_void)
        .unwrap_or(std::ptr::null())
}

/// Callback type for constructing C# Host objects.
/// The callback receives raw pointers to node metadata and is responsible for:
/// 1. Constructing a C# Host object from the provided data
/// 2. Adding the Host to the C# RefreshContext referenced by context_ptr
///
/// # Safety
/// - All pointer parameters must be immediately copied/consumed during the callback invocation
/// - String pointers (datacenter_ptr, rack_ptr) are only valid for the duration of the callback
/// - The callback must not store these pointers or access them after returning
/// - The callback must not throw exceptions across the FFI boundary
type ConstructCSharpHost = unsafe extern "C" fn(
    context_ptr: *mut c_void,
    id_bytes: FFIByteSlice<'_>,
    ip_bytes: FFIByteSlice<'_>,
    port: u16,
    datacenter: FFIStr<'_>,
    rack: FFIStr<'_>,
);

#[derive(Debug, Error)]
enum MetadataBridgeError {
    #[error("null string: {0}")]
    NullString(&'static str),

    #[error("utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] scylla_cql::serialize::SerializationError),

    #[error("token error: {0}")]
    Token(#[from] ClusterStateTokenError),
}

#[derive(Clone, Copy)]
enum ReplicaList {}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct CallbackState(FfiPtr<'static, ReplicaList>);
type OnReplicaPair = unsafe extern "C" fn(
    callback_state: CallbackState,
    host_id_bytes_as_ptr: *const u8,
    shard: i32,
);

/// Short‑lived helper that encapsulates the work of computing a token and
/// retrieving replica endpoints for a single partition key.
///
/// # Safety
/// - `context_ptr` must point to a valid C# RefreshContext that remains allocated during this call
/// - All string pointers passed to the callback are temporary and only valid during that invocation
/// - The callback must copy string data (e.g., via Marshal.PtrToStringUTF8) and byte arrays (IP, host ID) immediately.
/// - The callback must not throw exceptions; use Environment.FailFast on errors
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_fill_nodes(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    context_ptr: *mut c_void,
    callback: ConstructCSharpHost,
) {
    let cluster_state =
        ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

    for node in cluster_state.get_nodes_info() {
        // UUID as bytes
        let uuid_bytes = FFIByteSlice::new(node.host_id.as_bytes());

        // The octets() returns an owned stack array. We store it in outer-scope
        // variables so we can take a slice that outlives the match expression.
        let ip_bytes_storage_v4: [u8; 4];
        let ip_bytes_storage_v6: [u8; 16];

        // Serialize IP address to bytes
        let port = node.address.port();
        let ip_bytes_slice: &[u8] = match node.address.ip() {
            std::net::IpAddr::V4(ipv4) => {
                ip_bytes_storage_v4 = ipv4.octets();
                let bytes = &ip_bytes_storage_v4[..];
                tracing::trace!("[FFI] Node IPv4: {:?}, port: {}", bytes, port);
                bytes
            }
            std::net::IpAddr::V6(ipv6) => {
                ip_bytes_storage_v6 = ipv6.octets();
                let bytes = &ip_bytes_storage_v6[..];
                tracing::trace!("[FFI] Node IPv6: {:?}, port: {}", bytes, port);
                bytes
            }
        };

        let ip_bytes = FFIByteSlice::new(ip_bytes_slice);

        // Get datacenter (Option<String>)
        let dc_str = FFIStr::new(node.datacenter.as_deref().unwrap_or(""));

        // Get rack (Option<String>)
        let rack_str = FFIStr::new(node.rack.as_deref().unwrap_or(""));

        // Invoke the callback to construct and add the Host to the C# list object.
        // All pointers passed to the callback are only valid during this invocation.
        // The callback must copy all data immediately.
        unsafe {
            callback(context_ptr, uuid_bytes, ip_bytes, port, dc_str, rack_str);
        }
    }
}

struct RustReplicaBridge<'a> {
    cluster_state: &'a ClusterState,
    keyspace_name: &'a str,
    table_name: Option<&'a str>,
    token: Token,
}

impl<'a> RustReplicaBridge<'a> {
    fn pre_serialized_values_from(
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
    ) -> Result<PreSerializedValues, MetadataBridgeError> {
        let mut psv = PreSerializedValues::new();
        let csharp_val = CsharpSerializedValue::new(partition_key_ptr, partition_key_len);
        psv.add_value(csharp_val)?;
        Ok(psv)
    }

    // Helper: call the provided FFI callback for each replica in `replicas`.
    // Accept an iterator of (Arc<Node>, Shard) to match the caller's intent that
    // we don't need ownership of a collection, just the ability to iterate.
    fn callback_foreach_replica(
        replicas: impl Iterator<Item = (Arc<Node>, Shard)>,
        callback_state: CallbackState,
        callback: OnReplicaPair,
    ) {
        for (node, shard) in replicas {
            let host_id_bytes = node.host_id.as_bytes();
            unsafe {
                callback(callback_state, host_id_bytes.as_ptr(), shard as i32);
            }
        }
    }

    fn new_with_table(
        cluster_state_ptr: BridgedBorrowedSharedPtr<'a, ClusterState>,
        keyspace: CSharpStr<'a>,
        table: CSharpStr<'a>,
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
    ) -> Result<Self, MetadataBridgeError> {
        let cluster_state =
            ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

        // Parse once and store borrowed &str references (no allocation).
        let keyspace_name = keyspace
            .as_cstr_tied_to_a()
            .ok_or(MetadataBridgeError::NullString("keyspace"))?
            .to_str()?;
        let table_name = table
            .as_cstr_tied_to_a()
            .ok_or(MetadataBridgeError::NullString("table"))?
            .to_str()?;

        let psv = Self::pre_serialized_values_from(partition_key_ptr, partition_key_len)?;

        let token = cluster_state.compute_token_preserialized(
            keyspace_name,
            table_name,
            &psv.into_serialized_values(),
        )?;

        Ok(Self {
            cluster_state,
            keyspace_name,
            table_name: Some(table_name),
            token,
        })
    }

    fn new_with_partitioner(
        cluster_state_ptr: BridgedBorrowedSharedPtr<'a, ClusterState>,
        keyspace: CSharpStr<'a>,
        partition_key_ptr: CsharpValuePtr,
        partition_key_len: usize,
        partitioner: PartitionerName,
    ) -> Result<Self, MetadataBridgeError> {
        let cluster_state =
            ArcFFI::as_ref(cluster_state_ptr).expect("valid and non-null ClusterState pointer");

        let keyspace_name = keyspace
            .as_cstr_tied_to_a()
            .ok_or(MetadataBridgeError::NullString("keyspace"))?
            .to_str()?;

        let psv = Self::pre_serialized_values_from(partition_key_ptr, partition_key_len)?;
        let serialized_values = psv.into_serialized_values();

        let token = cluster_state
            .compute_token_preserialized_with_partitioner(&partitioner, &serialized_values)?;

        Ok(Self {
            cluster_state,
            keyspace_name,
            table_name: None,
            token,
        })
    }

    /// Retrieve replicas and call `callback` for each replica.
    ///
    /// This method is intentionally infallible: all validation (null/UTF-8 and
    /// token computation) is performed in the constructors (`new_with_table` /
    /// `new_with_partitioner`). At this point `get_replicas` just performs the
    /// replica lookup and synchronously invokes the provided FFI callback.
    fn get_replicas(&self, callback_state: CallbackState, callback: OnReplicaPair) {
        let table_name = self.table_name.unwrap_or("");
        let replicas =
            self.cluster_state
                .get_token_endpoints(self.keyspace_name, table_name, self.token);
        Self::callback_foreach_replica(replicas.into_iter(), callback_state, callback);
    }
}

/// For now unused - get replicas for a specific table and partition key.
/// It should allow us to seamlessly support tablet-aware replica retrieval in the future.
/// TODO: extend the C# Metadata.GetReplicas to accept the table name as a parameter.
#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_replicas(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace: CSharpStr<'_>,
    table: CSharpStr<'_>,
    partition_key_ptr: CsharpValuePtr,
    partition_key_len: usize,
    callback_state: CallbackState,
    callback: OnReplicaPair,
    exception_constructors: &ExceptionConstructors,
) -> FfiException {
    match RustReplicaBridge::new_with_table(
        cluster_state_ptr,
        keyspace,
        table,
        partition_key_ptr,
        partition_key_len,
    ) {
        Ok(bridge) => {
            bridge.get_replicas(callback_state, callback);
            FfiException::ok()
        }
        Err(e) => FfiException::from_rust_exception(exception_constructors, e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cluster_state_get_replicas_murmur3(
    cluster_state_ptr: BridgedBorrowedSharedPtr<'_, ClusterState>,
    keyspace: CSharpStr<'_>,
    partition_key_ptr: CsharpValuePtr,
    partition_key_len: usize,
    callback_state: CallbackState,
    callback: OnReplicaPair,
    exception_constructors: &ExceptionConstructors,
) -> FfiException {
    match RustReplicaBridge::new_with_partitioner(
        cluster_state_ptr,
        keyspace,
        partition_key_ptr,
        partition_key_len,
        PartitionerName::Murmur3,
    ) {
        Ok(bridge) => {
            bridge.get_replicas(callback_state, callback);
            FfiException::ok()
        }
        Err(e) => FfiException::from_rust_exception(exception_constructors, e),
    }
}

// convenience method to convert Rust errors to FfiException
impl FfiException {
    fn from_rust_exception<E: ToString>(constructors: &ExceptionConstructors, err: E) -> Self {
        FfiException::from_exception(
            constructors
                .rust_exception_constructor
                .construct_from_rust(err.to_string()),
        )
    }
}

// Provide a local helper method on the concrete `FfiPtr<'a, c_char>` instantiation
// that returns an Option<&'a CStr>
impl<'a> FfiPtr<'a, c_char> {
    fn as_cstr_tied_to_a(&self) -> Option<&'a CStr> {
        self.ptr.map(|ptr| unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}
