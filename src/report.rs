use std::path::Path;
use std::sync::Arc;

use serde::Serialize;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::{Error, ObjectTask, Result, TransferProgress};

#[derive(Clone)]
pub(crate) struct Report(Arc<Mutex<File>>);

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Record<'a> {
    Object {
        path: &'a str,
        bytes: u64,
        status: &'static str,
        error: Option<String>,
        cleanup_failed: bool,
    },
    Finished {
        progress: TransferProgress,
        error: Option<String>,
    },
}

impl Report {
    pub async fn create(path: &Path) -> Result<Self> {
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .map_err(Error::Report)?;
        Ok(Self(Arc::new(Mutex::new(file))))
    }

    pub async fn object(&self, task: &ObjectTask, result: &Result<()>) -> Result<()> {
        self.append(&Record::Object {
            path: &task.path,
            bytes: task.metadata.content_length(),
            status: result.as_ref().err().map_or("copied", status),
            error: result.as_ref().err().map(ToString::to_string),
            cleanup_failed: matches!(result, Err(Error::Cleanup { .. })),
        })
        .await
    }

    pub async fn finished(
        &self,
        progress: TransferProgress,
        result: &Result<TransferProgress>,
    ) -> Result<()> {
        self.append(&Record::Finished {
            progress,
            error: result.as_ref().err().map(ToString::to_string),
        })
        .await
    }

    async fn append(&self, record: &Record<'_>) -> Result<()> {
        let mut bytes = serde_json::to_vec(record)
            .map_err(|error| Error::Report(std::io::Error::other(error)))?;
        bytes.push(b'\n');
        let mut file = self.0.lock().await;
        file.write_all(&bytes).await.map_err(Error::Report)?;
        file.flush().await.map_err(Error::Report)
    }
}

fn status(error: &Error) -> &'static str {
    match error {
        Error::Cancelled => "cancelled",
        Error::CommitUnknown { .. } => "uncertain",
        Error::Cleanup { original, .. } => status(original),
        _ => "failed",
    }
}
