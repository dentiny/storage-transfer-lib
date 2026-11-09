use futures_util::TryStreamExt;
use opendal::{EntryMode, Operator};
use tokio::sync::{mpsc, watch};

use crate::{Error, ObjectTask, Result, TransferLimits, TransferProgress};

pub struct BucketProducer {
    source: Operator,
    sender: mpsc::Sender<ObjectTask>,
    list_page_size: usize,
    progress: Option<watch::Sender<TransferProgress>>,
}

impl BucketProducer {
    pub fn new(source: Operator, limits: TransferLimits) -> (Self, mpsc::Receiver<ObjectTask>) {
        let (sender, receiver) = mpsc::channel(limits.max_queued_objects());
        (
            Self {
                source,
                sender,
                list_page_size: limits.list_page_size(),
                progress: None,
            },
            receiver,
        )
    }

    pub(crate) fn with_progress(mut self, progress: watch::Sender<TransferProgress>) -> Self {
        self.progress = Some(progress);
        self
    }

    // Run alongside the consumer. Queue closure alone does not mean listing succeeded;
    // the caller must also check this result. Dropping this future stops production.
    pub async fn run(self) -> Result<()> {
        tokio::select! {
            biased;
            _ = self.sender.closed() => Err(Error::ConsumerClosed),
            result = self.produce() => result,
        }
    }

    async fn produce(&self) -> Result<()> {
        let mut lister = self
            .source
            .lister_with("/")
            .recursive(true)
            .limit(self.list_page_size)
            .await?;

        loop {
            // Reserve queue capacity before advancing the lister, including fetching another page.
            let permit = self
                .sender
                .reserve()
                .await
                .map_err(|_| Error::ConsumerClosed)?;
            let Some(entry) = lister.try_next().await? else {
                return Ok(());
            };
            let (path, metadata) = entry.into_parts();
            match metadata.mode() {
                EntryMode::FILE => {
                    if let Some(progress) = &self.progress {
                        progress.send_modify(|progress| {
                            progress.listed_objects += 1;
                        });
                    }
                    permit.send(ObjectTask { path, metadata });
                }
                EntryMode::DIR => continue,
                EntryMode::Unknown => return Err(Error::UnsupportedEntry(path)),
            }
        }
    }
}
