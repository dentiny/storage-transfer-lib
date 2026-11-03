use std::path::Path;
use std::sync::Arc;

use opendal::Operator;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;

use crate::operator::with_policies;
use crate::report::Report;
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
            source: with_policies(source.layer(layer.clone())),
            destination: with_policies(destination.layer(layer)),
            limits,
            progress,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<TransferProgress> {
        self.progress.subscribe()
    }

    // A mutable borrow allows one transfer at a time per engine.
    pub async fn run(&mut self, cancellation: &CancellationToken) -> Result<TransferProgress> {
        self.run_inner(cancellation, None).await
    }

    pub async fn run_with_report(
        &mut self,
        cancellation: &CancellationToken,
        path: &Path,
    ) -> Result<TransferProgress> {
        let report = Report::create(path).await?;
        let result = self.run_inner(cancellation, Some(report.clone())).await;
        let progress = *self.progress.borrow();
        let reported = report.finished(progress, &result).await;
        let progress = result?;
        reported?;
        Ok(progress)
    }

    async fn run_inner(
        &mut self,
        cancellation: &CancellationToken,
        report: Option<Report>,
    ) -> Result<TransferProgress> {
        self.progress.send_replace(TransferProgress::default());
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
            report,
        };
        for _ in 0..self.limits.worker_count() {
            tasks.spawn(worker.clone().run());
        }

        let mut failure = None;
        // After failure, drain every task so active copies can abort unfinished uploads.
        while let Some(result) = tasks.join_next().await {
            let result = result.unwrap_or_else(|error| Err(Error::Task(error)));
            if let Err(error) = result {
                cancellation.cancel();
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(*self.progress.borrow())
    }
}
