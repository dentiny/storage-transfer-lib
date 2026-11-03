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
