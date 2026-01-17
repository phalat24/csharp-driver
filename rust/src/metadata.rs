use crate::ffi::{ArcFFI, BridgedOwnedSharedPtr, FFI, FromArc};
use scylla::cluster::ClusterState;

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
