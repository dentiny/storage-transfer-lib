use std::sync::{Arc, Mutex};

use futures_util::{StreamExt, stream};
use http::{Method, Request, Response, StatusCode};
use opendal::{
    Buffer, BytesRange, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator,
    raw::BytesContentRange, services::S3,
};
use storage_transfer_lib::{TransferEngine, TransferLimits, TransferProgress};
use tokio::sync::Notify;

const BUFFER_SIZE: usize = 8 * 1024 * 1024;
const ETAG: &str = "\"test-etag\"";

#[derive(Clone, Copy)]
pub enum Fault {
    None,
    Listing,
    Copy,
    Panic,
    BlockRead,
}

#[derive(Default)]
struct ObjectState {
    written: usize,
    parts: usize,
    completed: bool,
}

struct State {
    objects: Vec<ObjectState>,
    pages: usize,
    ongoing: usize,
    peak: usize,
}

#[derive(Clone)]
pub struct TestTransfer {
    state: Arc<Mutex<State>>,
    objects: usize,
    fault: Fault,
    pub read_started: Arc<Notify>,
}

impl TestTransfer {
    pub fn new(objects: usize, fault: Fault) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                objects: (0..objects).map(|_| ObjectState::default()).collect(),
                pages: 0,
                ongoing: 0,
                peak: 0,
            })),
            objects,
            fault,
            read_started: Arc::new(Notify::new()),
        }
    }

    pub fn engine(&self, limits: TransferLimits) -> TransferEngine {
        let operator = |bucket| {
            Operator::new(
                S3::default()
                    .bucket(bucket)
                    .region("us-ashburn-1")
                    .endpoint("https://storage.invalid")
                    .access_key_id("test-key")
                    .secret_access_key("test-secret")
                    .disable_config_load()
                    .disable_ec2_metadata(),
            )
            .unwrap()
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(self.clone())),
            )
        };
        TransferEngine::new(operator("source"), operator("destination"), limits).unwrap()
    }

    pub fn assert_completed(&self, summary: TransferProgress, limits: TransferLimits) {
        let state = self.state.lock().unwrap();
        assert!(state.objects.iter().all(|object| object.completed));
        assert_eq!(summary.listed_objects, self.objects as u64);
        assert_eq!(summary.copied_objects, summary.listed_objects);
        assert_eq!(
            summary.copied_bytes,
            (0..self.objects).map(Self::size).sum::<usize>() as u64
        );
        assert_eq!(
            state.pages,
            self.objects.div_ceil(limits.list_page_size()).max(1)
        );
        assert!(state.peak <= limits.max_ongoing_http_requests());
        if self.objects > 1 && limits.max_ongoing_http_requests() > 1 {
            assert!(state.peak > 1);
        }
        assert_eq!(state.ongoing, 0);
    }

    pub fn assert_stopped(&self) {
        let state = self.state.lock().unwrap();
        assert_eq!(state.ongoing, 0);
        assert!(state.objects.iter().any(|object| !object.completed));
    }

    fn size(index: usize) -> usize {
        [0, 17, 2048, BUFFER_SIZE + 13][index % 4]
    }

    fn list(&self, query: &[(&str, &str)]) -> Response<Buffer> {
        let param = |name| {
            query
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| *value)
        };
        assert_eq!(param("list-type"), Some("2"));
        assert!(param("prefix").is_none());
        assert!(param("delimiter").is_none());
        let start: usize = param("continuation-token").unwrap_or("0").parse().unwrap();
        let limit: usize = param("max-keys").unwrap().parse().unwrap();
        assert!(limit > 0);
        self.state.lock().unwrap().pages += 1;
        if matches!(self.fault, Fault::Listing) && start > 0 {
            return Self::response(
                StatusCode::FORBIDDEN,
                "<Error><Code>AccessDenied</Code></Error>",
            );
        }
        let end = (start + limit).min(self.objects);
        let mut xml = format!(
            "<ListBucketResult><IsTruncated>{}</IsTruncated>",
            end < self.objects
        );
        if end < self.objects {
            xml.push_str(&format!(
                "<NextContinuationToken>{end}</NextContinuationToken>"
            ));
        }
        for index in start..end {
            xml.push_str(&format!(
                "<Contents><Key>objects/{index}</Key><Size>{}</Size><ETag>{ETAG}</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>",
                Self::size(index),
            ));
        }
        xml.push_str("</ListBucketResult>");
        Self::response(StatusCode::OK, xml)
    }

    fn stat(&self, index: usize, request: &Request<Buffer>) -> Response<Buffer> {
        assert_eq!(request.headers()["if-match"], ETAG);
        if index == 0 {
            match self.fault {
                Fault::Panic => panic!("injected worker panic"),
                Fault::Copy => return Self::response(StatusCode::PRECONDITION_FAILED, ""),
                _ => {}
            }
        }
        let mut response = Self::response(StatusCode::OK, "");
        response
            .headers_mut()
            .insert("content-length", Self::size(index).into());
        response
    }

    async fn read(&self, index: usize, request: &Request<Buffer>) -> Response<Buffer> {
        if matches!(self.fault, Fault::BlockRead) {
            self.read_started.notify_one();
            std::future::pending::<()>().await;
        }
        assert_eq!(request.headers()["if-match"], ETAG);
        let range: BytesRange = request.headers()["range"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let range = range.to_content_range(Self::size(index)).unwrap();
        assert!(range.len() <= BUFFER_SIZE);
        let bytes: Vec<u8> = range
            .clone()
            .map(|offset| ((index + offset) % 251) as u8)
            .collect();
        let mut response = Self::response(StatusCode::PARTIAL_CONTENT, bytes);
        let content_range = BytesContentRange::default()
            .with_range(range.start as u64, (range.end - 1) as u64)
            .with_size(Self::size(index) as u64);
        response
            .headers_mut()
            .insert("content-range", content_range.to_header().parse().unwrap());
        response
    }

    fn write(
        &self,
        index: usize,
        request: &Request<Buffer>,
        part: Option<usize>,
    ) -> Response<Buffer> {
        let mut state = self.state.lock().unwrap();
        let object = &mut state.objects[index];
        assert!(!object.completed);
        if let Some(part) = part {
            assert_eq!(part, object.parts + 1);
            object.parts += 1;
        } else {
            assert_eq!(request.headers()["if-none-match"], "*");
            object.completed = true;
        }
        assert!(request.body().len() <= BUFFER_SIZE);
        for (offset, byte) in request.body().to_bytes().iter().enumerate() {
            assert_eq!(*byte, ((index + object.written + offset) % 251) as u8);
        }
        object.written += request.body().len();
        if part.is_none() {
            assert_eq!(object.written, Self::size(index));
        }
        Self::response(StatusCode::OK, "")
    }

    fn complete(&self, index: usize, request: &Request<Buffer>) -> Response<Buffer> {
        assert_eq!(request.headers()["if-none-match"], "*");
        let mut state = self.state.lock().unwrap();
        let object = &mut state.objects[index];
        assert_eq!(object.written, Self::size(index));
        assert_eq!(object.parts, Self::size(index).div_ceil(BUFFER_SIZE));
        assert!(!object.completed);
        object.completed = true;
        Self::response(
            StatusCode::OK,
            "<CompleteMultipartUploadResult><ETag>test-etag</ETag></CompleteMultipartUploadResult>",
        )
    }

    fn response(status: StatusCode, body: impl Into<Buffer>) -> Response<Buffer> {
        let body = body.into();
        Response::builder()
            .status(status)
            .header("etag", ETAG)
            .header("content-length", body.len())
            .body(body)
            .unwrap()
    }
}

// Track a request until its response stream is dropped, including cancellation and panic.
struct ActiveRequest(Arc<Mutex<State>>);

impl ActiveRequest {
    fn new(state: Arc<Mutex<State>>) -> Self {
        {
            let mut state = state.lock().unwrap();
            state.ongoing += 1;
            state.peak = state.peak.max(state.ongoing);
        }
        Self(state)
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.lock().unwrap().ongoing -= 1;
    }
}

impl HttpTransport for TestTransfer {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let active = ActiveRequest::new(self.state.clone());
        tokio::task::yield_now().await;
        let mut query: Vec<_> =
            form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes()).collect();
        query.sort_unstable();
        let params: Vec<_> = query
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
            .collect();
        let path: Vec<_> = request.uri().path().split('/').collect();
        let response = match (request.method(), path.as_slice(), params.as_slice()) {
            (&Method::GET, ["", "source"], params) => self.list(params),
            (&Method::HEAD, ["", "source", "objects", index], []) => {
                self.stat(index.parse().unwrap(), &request)
            }
            (&Method::GET, ["", "source", "objects", index], []) => {
                self.read(index.parse().unwrap(), &request).await
            }
            (&Method::PUT, ["", "destination", "objects", index], []) => {
                self.write(index.parse().unwrap(), &request, None)
            }
            (&Method::POST, ["", "destination", "objects", _], [("uploads", "")]) => {
                Self::response(
                    StatusCode::OK,
                    "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
                )
            }
            (
                &Method::PUT,
                ["", "destination", "objects", index],
                [("partNumber", part), ("uploadId", "test-upload")],
            ) => self.write(
                index.parse().unwrap(),
                &request,
                Some(part.parse().unwrap()),
            ),
            (
                &Method::POST,
                ["", "destination", "objects", index],
                [("uploadId", "test-upload")],
            ) => self.complete(index.parse().unwrap(), &request),
            (&Method::DELETE, ["", "destination", "objects", _], [("uploadId", "test-upload")]) => {
                Self::response(StatusCode::NO_CONTENT, "")
            }
            _ => panic!(
                "unexpected S3 request: {} {}",
                request.method(),
                request.uri()
            ),
        };
        let (parts, body) = response.into_parts();
        let size = body.len() as u64;
        let body = stream::iter([Ok(body)]).map(move |chunk| {
            let _ = &active;
            chunk
        });
        Ok(Response::from_parts(parts, HttpBody::new(body, Some(size))))
    }
}
