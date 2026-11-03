use opendal::Metadata;

// Describes one file object queued for copying; contains no object data.
// The producer filters out directory entries before creating tasks.
#[derive(Clone, Debug)]
pub struct ObjectTask {
    // Object key relative to the source bucket root.
    pub path: String,
    // Metadata returned by listing, such as size and ETag.
    pub metadata: Metadata,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct TransferProgress {
    pub listed_objects: u64,
    // Counted after the destination data and metadata have been verified.
    pub copied_objects: u64,
    pub copied_bytes: u64,
}
