use futures_util::TryStreamExt;
use opendal::{Metadata, Operator};
use sha2::{Digest, Sha256};

use crate::common::Sha256Digest;
use crate::validation::validate_destination;
use crate::{CancellationToken, Error, Result};

pub(crate) async fn verify_object(
    destination: &Operator,
    path: &str,
    source: &Metadata,
    expected: Sha256Digest,
    cancellation: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = async {
            let metadata = destination.stat(path).await?;
            let etag = validate_destination(source, &metadata, path)?;
            // Without `chunk`, OpenDAL streams one conditional GET instead of
            // collecting a separate response for every multipart-sized range.
            let reader = destination.reader_with(path).if_match(etag).await?;
            let mut stream = reader.into_stream(0..metadata.content_length()).await?;
            let mut hash = Sha256::new();
            let mut offset = 0;
            while let Some(buffer) = stream.try_next().await? {
                if buffer.len() as u64 > metadata.content_length() - offset {
                    return Err(Error::VerificationFailed(path.into()));
                }
                offset += buffer.len() as u64;
                for chunk in buffer { hash.update(chunk); }
            }
            if offset != metadata.content_length() { return Err(Error::VerificationFailed(path.into())); }
            let actual: Sha256Digest = hash.finalize().into();
            if actual != expected { return Err(Error::VerificationFailed(path.into())); }
            Ok(())
        } => result,
    }
}
