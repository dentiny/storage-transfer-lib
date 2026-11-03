use std::sync::Arc;

use opendal::Operator;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;

use crate::validation::validate_transfer;
use crate::worker::CopyWorker;
use crate::{
    BucketProducer, CancellationToken, Error, HttpConcurrencyLimitLayer, Result, TransferLimits,
    TransferProgress,
};

pub struct TransferEngine {
    source: Operator,
    destination: Operator,
    limits: TransferLimits,
    progress: watch::Sender<TransferProgress>,
}

impl TransferEngine {
    pub fn new(source: Operator, destination: Operator, limits: TransferLimits) -> Result<Self> {
        validate_transfer(&source, &destination)?;
        let layer = HttpConcurrencyLimitLayer::new(limits.max_ongoing_http_requests())?;
        let (progress, _) = watch::channel(TransferProgress::default());
        Ok(Self {
            source: source.layer(layer.clone()),
            destination: destination.layer(layer),
            limits,
            progress,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<TransferProgress> {
        self.progress.subscribe()
    }

    // A mutable borrow allows one transfer at a time per engine.
    pub async fn run(&mut self, cancellation: &CancellationToken) -> Result<TransferProgress> {
        self.progress.send_replace(TransferProgress::default());
        if cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let cancellation = cancellation.child_token();
        let (producer, queue) = BucketProducer::new(self.source.clone(), self.limits);
        let producer = producer.with_progress(self.progress.clone());
        let mut tasks = JoinSet::new();
        let producer_cancellation = cancellation.clone();
        tasks.spawn(async move {
            tokio::select! {
                biased;
                _ = producer_cancellation.cancelled() => Err(Error::Cancelled),
                result = producer.run() => result,
            }
        });
        // Keep the receiver alive until all tasks exit, preserving the original worker error.
        let worker = CopyWorker {
            source: self.source.clone(),
            destination: self.destination.clone(),
            queue: Arc::new(Mutex::new(queue)),
            cancellation: cancellation.clone(),
            progress: self.progress.clone(),
        };
        for _ in 0..self.limits.worker_count() {
            tasks.spawn(worker.clone().run());
        }

        let mut first_error = None;
        // After failure, drain every task so active copies can abort unfinished uploads.
        while let Some(result) = tasks.join_next().await {
            let result = result.unwrap_or_else(|error| Err(Error::Task(error)));
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
                cancellation.cancel();
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(*self.progress.borrow())
    }
}
