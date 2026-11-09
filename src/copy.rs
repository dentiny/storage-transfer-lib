use futures_util::TryStreamExt;
use opendal::options::WriteOptions;
use opendal::{Buffer, Metadata, Operator, Reader, Writer};
use sha2::{Digest, Sha256};

use crate::common::Sha256Digest;
use crate::config::COPY_BUFFER_SIZE;
use crate::operator::with_policies;
use crate::validation::{validate_config, validate_copy, validate_metadata, validate_source};
use crate::verify::verify_object;
use crate::{
    CancellationToken, Error, HttpConcurrencyLimitLayer, ObjectTask, Result, TransferConfig,
};

// Apply a shared HTTP limiter and retry/timeout policies to this copy.
pub async fn copy_object(
    source: &Operator,
    destination: &Operator,
    task: &ObjectTask,
    config: &TransferConfig,
    cancellation: &CancellationToken,
) -> Result<()> {
    validate_config(config)?;
    let layer = HttpConcurrencyLimitLayer::new(config.limits.max_ongoing_http_requests())?;
    copy_prepared(
        &with_policies(source.clone().layer(layer.clone()), config),
        &with_policies(destination.clone().layer(layer), config),
        task,
        cancellation,
    )
    .await
}

pub(crate) async fn copy_prepared(
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
    let reader = source.reader_with(&task.path).if_match(etag).await?;
    let mut writer = destination
        .writer_options(&task.path, write_options(&metadata))
        .await?;
    let digest = match write_data(&reader, &mut writer, task, cancellation).await {
        Ok(digest) => digest,
        Err(error) => return abort(&mut writer, error).await,
    };

    // Drain close under the operator's retry/timeout policy before using the writer again.
    if let Err(error) = writer.close().await {
        return abort(&mut writer, error.into()).await;
    }
    verify_object(destination, &task.path, &metadata, digest, cancellation).await
}

async fn write_data(
    reader: &Reader,
    writer: &mut Writer,
    task: &ObjectTask,
    cancellation: &CancellationToken,
) -> Result<Sha256Digest> {
    let mut hash = Sha256::new();
    let size = task.metadata.content_length();
    if size > 0 {
        let mut offset = size.min(COPY_BUFFER_SIZE as u64);
        let mut buffer = read_part(reader, 0..offset, task, cancellation).await?;
        loop {
            for chunk in buffer.clone() {
                hash.update(chunk);
            }
            if offset == size {
                writer.write(buffer).await?;
                break;
            }
            let end = (offset + COPY_BUFFER_SIZE as u64).min(size);
            // Read at most one part ahead. Each bounded GET is fully consumed
            // before it returns, so neither side waits on a permit held by a
            // suspended response body, even with a single shared HTTP permit.
            // Drain the write on read failure/cancellation to retain the state
            // needed by the caller's multipart abort.
            let prefetch_cancellation = cancellation.child_token();
            let write = async {
                let result = writer.write(buffer).await;
                if result.is_err() {
                    prefetch_cancellation.cancel();
                }
                result
            };
            let (written, next) = tokio::join!(
                write,
                read_part(reader, offset..end, task, &prefetch_cancellation),
            );
            written?;
            buffer = next?;
            offset = end;
        }
    }
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    Ok(hash.finalize().into())
}

async fn read_part(
    reader: &Reader,
    range: std::ops::Range<u64>,
    task: &ObjectTask,
    cancellation: &CancellationToken,
) -> Result<Buffer> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = async {
            let size = (range.end - range.start) as usize;
            let mut bytes = Vec::with_capacity(size);
            let mut stream = reader.clone().into_stream(range).await?;
            while let Some(buffer) = stream.try_next().await? {
                if buffer.len() > size - bytes.len() {
                    return Err(Error::SourceChanged(task.path.clone()));
                }
                // Compact incrementally: retaining zero-copy HTTP slices can
                // pin much larger backing allocations until upload finishes.
                for chunk in buffer {
                    bytes.extend_from_slice(&chunk);
                }
            }
            if bytes.len() != size {
                return Err(Error::SourceChanged(task.path.clone()));
            }
            Ok(Buffer::from(bytes))
        } => result,
    }
}

async fn abort(writer: &mut Writer, original: Error) -> Result<()> {
    let _ = writer.abort().await;
    Err(original)
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
