use opendal::raw::normalize_path;
use opendal::{Capability, Metadata, Operator};

use crate::config::{COPY_BUFFER_SIZE, MAX_COPY_PARTS};
use crate::{Error, ObjectTask, Result, TransferConfig};

pub(crate) fn validate_copy(
    source: &Operator,
    destination: &Operator,
    task: &ObjectTask,
) -> Result<()> {
    validate_object(task)?;
    validate_operators(source, destination)?;
    validate_object_size(
        task.metadata.content_length(),
        destination.info().capability(),
    )
}

pub(crate) fn validate_transfer(source: &Operator, destination: &Operator) -> Result<()> {
    validate_operators(source, destination)?;
    if !source.info().capability().list {
        return Err(Error::Unsupported(
            "source must support bucket listing".into(),
        ));
    }
    Ok(())
}

fn validate_operators(source: &Operator, destination: &Operator) -> Result<()> {
    validate_buckets(source, destination)?;
    let read = source.info().capability();
    let write = destination.info().capability();
    validate_conditions(read, write)?;
    validate_multipart(write)?;
    if !write.stat || !write.read_with_if_match {
        return Err(Error::Unsupported(
            "destination must support conditional reads for verification".into(),
        ));
    }
    Ok(())
}

fn validate_object(task: &ObjectTask) -> Result<()> {
    if !task.metadata.is_file() || task.path.ends_with('/') {
        return Err(Error::UnsupportedEntry(task.path.clone()));
    }
    if normalize_path(&task.path) != task.path {
        return Err(Error::Unsupported(
            "object key cannot be preserved by OpenDAL path normalization".into(),
        ));
    }
    Ok(())
}

fn validate_buckets(source: &Operator, destination: &Operator) -> Result<()> {
    let source_info = source.info();
    let destination_info = destination.info();
    // S3 operator names identify the bucket. Conservatively reject overlap
    // even across endpoints with the same bucket name: listing and writing
    // intersecting key spaces can otherwise recursively copy our own output.
    if source_info.name() == destination_info.name()
        && (source_info.root().starts_with(&destination_info.root())
            || destination_info.root().starts_with(&source_info.root()))
    {
        return Err(Error::Unsupported(
            "source and destination directories overlap in the same bucket".into(),
        ));
    }
    if source.info().scheme() != "s3" || destination.info().scheme() != "s3" {
        return Err(Error::Unsupported(
            "only S3-compatible buckets are supported".into(),
        ));
    }
    Ok(())
}

fn validate_conditions(read: Capability, write: Capability) -> Result<()> {
    if !read.read_with_if_match || !read.stat_with_if_match || !write.write_with_if_not_exists {
        return Err(Error::Unsupported(
            "conditional source reads and create-only writes are required".into(),
        ));
    }
    Ok(())
}

fn validate_multipart(write: Capability) -> Result<()> {
    if !write.write_can_multi
        || !write.write_can_empty
        || write
            .write_multi_min_size
            .is_some_and(|min| COPY_BUFFER_SIZE < min)
        || write
            .write_multi_max_size
            .is_some_and(|max| COPY_BUFFER_SIZE > max)
    {
        return Err(Error::Unsupported(
            "destination cannot use the configured multipart buffer size".into(),
        ));
    }
    Ok(())
}

fn validate_object_size(size: u64, write: Capability) -> Result<()> {
    let max_size = (COPY_BUFFER_SIZE as u64 * MAX_COPY_PARTS).min(
        write
            .write_total_max_size
            .map(|size| size as u64)
            .unwrap_or(u64::MAX),
    );
    if size > max_size {
        return Err(Error::Unsupported(format!(
            "object size {size} exceeds the supported maximum {max_size}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_source<'a>(metadata: &'a Metadata, task: &ObjectTask) -> Result<&'a str> {
    if !metadata.is_file() || metadata.content_length() != task.metadata.content_length() {
        return Err(Error::SourceChanged(task.path.clone()));
    }
    metadata
        .etag()
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| {
            Error::Unsupported("source must provide an ETag for conditional range reads".into())
        })
}

pub(crate) fn validate_metadata(metadata: &Metadata, capability: Capability) -> Result<()> {
    for (name, present, supported) in [
        (
            "content type",
            metadata.content_type().is_some(),
            capability.write_with_content_type,
        ),
        (
            "content encoding",
            metadata.content_encoding().is_some(),
            capability.write_with_content_encoding,
        ),
        (
            "content disposition",
            metadata.content_disposition().is_some(),
            capability.write_with_content_disposition,
        ),
        (
            "cache control",
            metadata.cache_control().is_some(),
            capability.write_with_cache_control,
        ),
        (
            "user metadata",
            metadata.user_metadata().is_some(),
            capability.write_with_user_metadata,
        ),
    ] {
        if present && !supported {
            return Err(Error::Unsupported(format!(
                "destination cannot preserve {name}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_destination<'a>(
    source: &Metadata,
    destination: &'a Metadata,
    path: &str,
) -> Result<&'a str> {
    if !destination.is_file() || destination.content_length() != source.content_length() {
        return Err(Error::VerificationFailed(path.into()));
    }
    for (expected, actual) in [
        (source.content_type(), destination.content_type()),
        (source.content_encoding(), destination.content_encoding()),
        (
            source.content_disposition(),
            destination.content_disposition(),
        ),
        (source.cache_control(), destination.cache_control()),
    ] {
        if expected.is_some() && expected != actual {
            return Err(Error::VerificationFailed(path.into()));
        }
    }
    if let Some(expected) = source.user_metadata() {
        let actual = destination.user_metadata();
        for (key, value) in expected {
            if actual.as_ref().and_then(|values| values.get(key)) != Some(value) {
                return Err(Error::VerificationFailed(path.into()));
            }
        }
    }
    destination
        .etag()
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| Error::VerificationFailed(path.into()))
}

pub(crate) fn validate_config(config: &TransferConfig) -> Result<()> {
    for (field, duration) in [
        ("operation_timeout", config.operation_timeout),
        ("io_timeout", config.io_timeout),
        ("retry_delay", config.retry_delay),
        ("max_retry_delay", config.max_retry_delay),
    ] {
        if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
            return Err(Error::InvalidConfig(field));
        }
    }
    if config.max_retry_delay < config.retry_delay {
        return Err(Error::InvalidConfig(
            "max_retry_delay must be >= retry_delay",
        ));
    }
    Ok(())
}
