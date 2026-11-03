use futures_util::TryStreamExt;
use opendal::{EntryMode, Metadata, Operator};
use tokio::sync::mpsc;

use crate::{ProducerError, TransferLimits};

#[derive(Clone, Debug)]
pub struct ObjectTask {
    pub path: String,
    pub metadata: Metadata,
}

pub struct BucketProducer {
    source: Operator,
    sender: mpsc::Sender<ObjectTask>,
}

impl BucketProducer {
    pub fn new(source: Operator, limits: TransferLimits) -> (Self, mpsc::Receiver<ObjectTask>) {
        let (sender, receiver) = mpsc::channel(limits.max_queued_objects());
        (Self { source, sender }, receiver)
    }

    // Run alongside the consumer. Queue closure alone does not mean listing succeeded;
    // the caller must also check this result. Dropping this future stops production.
    pub async fn run(self) -> Result<(), ProducerError> {
        tokio::select! {
            biased;
            _ = self.sender.closed() => Err(ProducerError::ConsumerClosed),
            result = self.produce() => result,
        }
    }

    async fn produce(&self) -> Result<(), ProducerError> {
        if self.source.info().root() != "/" {
            return Err(ProducerError::InvalidRoot);
        }
        let mut lister = self
            .source
            .lister_with("/")
            .recursive(true)
            .limit(self.sender.max_capacity().min(1000))
            .await?;

        loop {
            // Reserve queue capacity before advancing the lister, including fetching another page.
            let permit = self
                .sender
                .reserve()
                .await
                .map_err(|_| ProducerError::ConsumerClosed)?;
            let Some(entry) = lister.try_next().await? else {
                return Ok(());
            };
            let (path, metadata) = entry.into_parts();
            if metadata.mode() == EntryMode::Unknown {
                return Err(ProducerError::UnsupportedEntry(path));
            }
            // Retain directory-marker entries as well as files, including zero-byte objects.
            permit.send(ObjectTask { path, metadata });
        }
    }
}
