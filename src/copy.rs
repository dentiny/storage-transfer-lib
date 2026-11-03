use opendal::options::WriteOptions;
use opendal::{Metadata, Operator};

use crate::config::COPY_BUFFER_SIZE;
use crate::validation::{validate_copy, validate_metadata, validate_source};
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
    let etag = validate_source(&metadata, task)?;
    validate_metadata(&metadata, destination.info().capability())?;
    let options = write_options(&metadata);
    let reader = source
        .reader_with(&task.path)
        .if_match(etag)
        .chunk(COPY_BUFFER_SIZE)
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

fn write_options(metadata: &Metadata) -> WriteOptions {
    WriteOptions {
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
    }
}
