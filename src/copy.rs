use opendal::options::WriteOptions;
use opendal::raw::normalize_path;
use opendal::{Capability, Metadata, Operator};

use crate::config::{COPY_BUFFER_SIZE, MAX_COPY_PARTS};
use crate::{CancellationToken, Error, ObjectTask, Result};

// Copy one object to the same key. Both operators must share the HTTP limiter.
// Cancellation stops pending work and attempts to abort this copy's unfinished upload.
pub async fn copy_object(
    source: &Operator,
    destination: &Operator,
    task: &ObjectTask,
    cancellation: &CancellationToken,
) -> Result<()> {
    validate_copy(source, destination, task)?;
    let mut stat = source.stat_with(&task.path);
    if let Some(etag) = task.metadata.etag() {
        stat = stat.if_match(etag);
    }
    let metadata = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        result = stat => result?,
    };
    if !metadata.is_file() || metadata.content_length() != task.metadata.content_length() {
        return Err(Error::SourceChanged(task.path.clone()));
    }
    let etag = metadata
        .etag()
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| {
            Error::Unsupported("source must provide an ETag for conditional range reads".into())
        })?;
    let options = write_options(&metadata, destination.info().capability())?;
    let reader = source
        .reader_with(&task.path)
        .if_match(etag)
        .concurrent(1)
        .await?;
    let mut writer = destination.writer_options(&task.path, options).await?;

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = async {
            let size = metadata.content_length();
            let mut offset = 0;
            while offset < size {
                let end = (offset + COPY_BUFFER_SIZE as u64).min(size);
                // Finish the GET body before requesting a PUT permit, so H=1 can progress.
                let buffer = reader.read(offset..end).await?;
                if buffer.len() as u64 != end - offset {
                    return Err(Error::SourceChanged(task.path.clone()));
                }
                writer.write(buffer).await?;
                offset = end;
            }
            // This also creates an empty object when size is zero.
            writer.close().await?;
            Ok(())
        } => result,
    };
    if let Err(original) = result {
        if let Err(cleanup) = writer.abort().await {
            return Err(Error::Cleanup {
                original: Box::new(original),
                cleanup,
            });
        }
        return Err(original);
    }
    Ok(())
}

fn validate_copy(source: &Operator, destination: &Operator, task: &ObjectTask) -> Result<()> {
    if !task.metadata.is_file() || task.path.ends_with('/') {
        return Err(Error::UnsupportedEntry(task.path.clone()));
    }
    if normalize_path(&task.path) != task.path {
        return Err(Error::Unsupported(
            "object key cannot be preserved by OpenDAL path normalization".into(),
        ));
    }
    if source.info().root() != "/" || destination.info().root() != "/" {
        return Err(Error::InvalidRoot);
    }
    if source.info().scheme() != "s3" || destination.info().scheme() != "s3" {
        return Err(Error::Unsupported(
            "only S3-compatible buckets are supported".into(),
        ));
    }
    let read = source.info().capability();
    let write = destination.info().capability();
    if !read.read_with_if_match || !read.stat_with_if_match || !write.write_with_if_not_exists {
        return Err(Error::Unsupported(
            "conditional source reads and create-only writes are required".into(),
        ));
    }
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
    let size = task.metadata.content_length();
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

fn write_options(metadata: &Metadata, capability: Capability) -> Result<WriteOptions> {
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
    Ok(WriteOptions {
        if_not_exists: true,
        chunk: Some(COPY_BUFFER_SIZE),
        concurrent: 1,
        content_type: metadata.content_type().map(str::to_owned),
        content_encoding: metadata.content_encoding().map(str::to_owned),
        content_disposition: metadata.content_disposition().map(str::to_owned),
        cache_control: metadata.cache_control().map(str::to_owned),
        user_metadata: metadata.user_metadata().map(|values| {
            values
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect()
        }),
        ..Default::default()
    })
}
