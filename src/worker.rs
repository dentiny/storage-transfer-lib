use std::sync::Arc;

use opendal::Operator;
use tokio::sync::{Mutex, mpsc, watch};

use crate::{CancellationToken, Error, ObjectTask, Result, TransferProgress, copy_object};

#[derive(Clone)]
pub(crate) struct CopyWorker {
    pub source: Operator,
    pub destination: Operator,
    pub queue: Arc<Mutex<mpsc::Receiver<ObjectTask>>>,
    pub cancellation: CancellationToken,
    pub progress: watch::Sender<TransferProgress>,
}

impl CopyWorker {
    pub async fn run(self) -> Result<()> {
        loop {
            let task = tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(Error::Cancelled),
                task = async { self.queue.lock().await.recv().await } => task,
            };
            let Some(task) = task else {
                return Ok(());
            };
            copy_object(&self.source, &self.destination, &task, &self.cancellation).await?;
            self.progress.send_modify(|progress| {
                progress.copied_objects += 1;
                progress.copied_bytes += task.metadata.content_length();
            });
        }
    }
}
