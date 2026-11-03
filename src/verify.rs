use opendal::{Metadata, Operator};
use sha2::{Digest, Sha256};

use crate::common::Sha256Digest;
use crate::config::COPY_BUFFER_SIZE;
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
            let reader = destination.reader_with(path).if_match(etag).chunk(COPY_BUFFER_SIZE).concurrent(1).await?;
            let mut hash = Sha256::new();
            let mut offset = 0;
            while offset < metadata.content_length() {
                let end = (offset + COPY_BUFFER_SIZE as u64).min(metadata.content_length());
                let buffer = reader.read(offset..end).await?;
                if buffer.len() as u64 != end - offset {
                    return Err(Error::VerificationFailed(path.into()));
                }
                for chunk in buffer { hash.update(chunk); }
                offset = end;
            }
            let actual: Sha256Digest = hash.finalize().into();
            if actual != expected { return Err(Error::VerificationFailed(path.into())); }
            Ok(())
        } => result,
    }
}
