use std::sync::Arc;

use opendal::Operator;
use tokio::sync::{Mutex, mpsc, watch};

use crate::copy::copy_prepared;
use crate::report::Report;

use crate::{CancellationToken, Error, ObjectTask, Result, TransferProgress};

#[derive(Clone)]
pub(crate) struct CopyWorker {
    pub source: Operator,
    pub destination: Operator,
    pub queue: Arc<Mutex<mpsc::Receiver<ObjectTask>>>,
    pub cancellation: CancellationToken,
    pub progress: watch::Sender<TransferProgress>,
    pub report: Option<Report>,
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
            let mut result =
                copy_prepared(&self.source, &self.destination, &task, &self.cancellation).await;
            if result.is_ok() {
                self.progress.send_modify(|progress| {
                    progress.copied_objects += 1;
                    progress.copied_bytes += task.metadata.content_length();
                });
            }
            if let Some(report) = &self.report {
                let reported = report.object(&task, &result).await;
                result = crate::error::combine(result, reported);
            }
            result?;
        }
    }
}
