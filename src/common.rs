use opendal::Metadata;

#[derive(Clone, Debug)]
pub struct ObjectTask {
    pub path: String,
    pub metadata: Metadata,
}
