use std::sync::{Arc, Mutex};

use futures_util::{StreamExt, stream};
use http::{Method, Request, Response, StatusCode};
use opendal::{
    Buffer, BytesRange, HttpBody, HttpTransport, HttpTransporter, MetadataBuilder,
    OperationContext, Operator, raw::BytesContentRange, services::S3,
};
use storage_transfer_lib::{
    CancellationToken, ObjectTask, Result, TransferConfig, TransferEngine, TransferLimits,
    copy_object,
};
use tokio::sync::Notify;

pub const BUFFER_SIZE: usize = 8 * 1024 * 1024;
const ETAG: &str = "\"test-etag\"";
const METADATA: [(&str, &str); 5] = [
    ("content-type", "application/octet-stream"),
    ("content-encoding", "identity"),
    ("content-disposition", "attachment"),
    ("cache-control", "max-age=60"),
    ("x-amz-meta-owner", "test"),
];

#[derive(Clone, Copy)]
pub enum Fault {
    None,
    Listing,
    Copy,
    Panic,
    BlockRead,
    ExistingDestination,
    Throttled,
    CorruptDestination,
    UnavailableDestination,
    SourceChanged,
    TimeoutRead,
    AbortHang,
    AbortDenied,
}

#[derive(Default)]
struct ObjectState {
    // Payload bytes checked across single PUTs and multipart uploads.
    written: usize,
    // Number of source range GET requests served.
    reads: usize,
    parts: usize,
    completed: bool,
    aborted: bool,
    // Existing bytes used to verify create-only writes; None means the destination is absent.
    existing: Option<&'static [u8]>,
}

struct State {
    objects: Vec<ObjectState>,
    ongoing: usize,
    peak: usize,
    failed_reads: usize,
    failed_writes: usize,
    aborts: usize,
}

#[derive(Clone)]
pub struct TestBucket {
    state: Arc<Mutex<State>>,
    sizes: Arc<[usize]>,
    fault: Fault,
    pub read_started: Arc<Notify>,
    source_prefix: String,
    destination_prefix: String,
}

impl TestBucket {
    pub fn new(sizes: &[usize], fault: Fault) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                objects: sizes
                    .iter()
                    .map(|_| ObjectState {
                        existing: matches!(fault, Fault::ExistingDestination)
                            .then_some(b"original destination data"),
                        ..ObjectState::default()
                    })
                    .collect(),
                ongoing: 0,
                peak: 0,
                failed_reads: 0,
                failed_writes: 0,
                aborts: 0,
            })),
            sizes: sizes.into(),
            fault,
            read_started: Arc::new(Notify::new()),
            source_prefix: String::new(),
            destination_prefix: String::new(),
        }
    }

    pub fn with_prefixes(mut self, source: &str, destination: &str) -> Self {
        self.source_prefix = storage_transfer_lib::normalize_directory_prefix(source).unwrap();
        self.destination_prefix =
            storage_transfer_lib::normalize_directory_prefix(destination).unwrap();
        self
    }

    fn operators(&self) -> (Operator, Operator) {
        let operator = |bucket, prefix: &str| {
            Operator::new(
                S3::default()
                    .bucket(bucket)
                    .root(prefix)
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
        (
            operator("source", &self.source_prefix),
            operator("destination", &self.destination_prefix),
        )
    }

    pub fn engine(&self, limits: TransferLimits) -> TransferEngine {
        let (source, destination) = self.operators();
        TransferEngine::new(
            source,
            destination,
            TransferConfig {
                limits,
                ..TransferConfig::default()
            },
        )
        .unwrap()
    }

    // Copy one fixture object with a single HTTP permit shared by both operators.
    pub async fn copy(&self, index: usize) -> Result<()> {
        let (source, destination) = self.operators();
        let mut metadata = MetadataBuilder::file(self.sizes[index] as u64);
        metadata.etag(ETAG);
        let task = ObjectTask {
            path: format!("objects/{index}"),
            metadata: metadata.build(),
        };
        copy_object(
            &source,
            &destination,
            &task,
            &TransferConfig {
                limits: TransferLimits::new(1, 1).unwrap(),
                ..TransferConfig::default()
            },
            &CancellationToken::new(),
        )
        .await
    }

    pub fn assert_completed(&self, http_limit: usize) {
        let state = self.state.lock().unwrap();
        for (object, &size) in state.objects.iter().zip(self.sizes.iter()) {
            assert!(object.completed);
            assert_eq!(object.written, size);
            assert_eq!(object.reads, size.div_ceil(BUFFER_SIZE));
            assert_eq!(
                object.parts,
                if size > BUFFER_SIZE {
                    size.div_ceil(BUFFER_SIZE)
                } else {
                    0
                }
            );
            assert!(!object.aborted);
        }
        assert_eq!(state.ongoing, 0);
        assert!(state.peak <= http_limit);
        if self.sizes.len() > 1 && http_limit > 1 {
            assert!(state.peak > 1);
        }
    }

    pub fn assert_destination_unchanged(&self) {
        let state = self.state.lock().unwrap();
        for (object, &size) in state.objects.iter().zip(self.sizes.iter()) {
            assert_eq!(
                object.existing,
                Some(b"original destination data".as_slice())
            );
            assert!(!object.completed);
            assert_eq!(object.aborted, size > BUFFER_SIZE);
        }
        assert_eq!(state.ongoing, 0);
    }

    pub fn assert_stopped(&self) {
        let state = self.state.lock().unwrap();
        assert_eq!(state.ongoing, 0);
        assert!(state.objects.iter().any(|object| !object.completed));
    }

    pub fn assert_failures(&self, reads: usize, writes: usize, aborts: usize) {
        let state = self.state.lock().unwrap();
        assert_eq!(
            (state.failed_reads, state.failed_writes, state.aborts),
            (reads, writes, aborts)
        );
        assert_eq!(state.ongoing, 0);
    }

    fn list(&self, query: &[(&str, &str)]) -> Response<Buffer> {
        let param = |name| {
            query
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| *value)
        };
        assert_eq!(param("list-type"), Some("2"));
        assert_eq!(
            param("prefix"),
            (!self.source_prefix.is_empty()).then_some(self.source_prefix.as_str())
        );
        assert!(param("delimiter").is_none());
        let start: usize = param("continuation-token").unwrap_or("0").parse().unwrap();
        let limit: usize = param("max-keys").unwrap().parse().unwrap();
        assert!(limit > 0);
        if matches!(self.fault, Fault::Listing) && start > 0 {
            return Self::response(
                StatusCode::FORBIDDEN,
                "<Error><Code>AccessDenied</Code></Error>",
            );
        }
        let end = (start + limit).min(self.sizes.len());
        let mut xml = format!(
            "<ListBucketResult><IsTruncated>{}</IsTruncated>",
            end < self.sizes.len()
        );
        if end < self.sizes.len() {
            xml.push_str(&format!(
                "<NextContinuationToken>{end}</NextContinuationToken>"
            ));
        }
        for index in start..end {
            xml.push_str(&format!(
                "<Contents><Key>{}objects/{index}</Key><Size>{}</Size><ETag>{ETAG}</ETag><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>",
                self.source_prefix, self.sizes[index],
            ));
        }
        xml.push_str("</ListBucketResult>");
        Self::response(StatusCode::OK, xml)
    }

    fn stat(&self, index: usize, destination: bool, request: &Request<Buffer>) -> Response<Buffer> {
        if destination {
            if !self.state.lock().unwrap().objects[index].completed {
                return Self::response(StatusCode::NOT_FOUND, "");
            }
        } else {
            assert_eq!(request.headers()["if-match"], ETAG);
        }
        if index == 0 && !destination {
            match self.fault {
                Fault::Panic => panic!("injected worker panic"),
                Fault::Copy => return Self::response(StatusCode::PRECONDITION_FAILED, ""),
                _ => {}
            }
        }
        let mut response = Self::response(StatusCode::OK, "");
        response
            .headers_mut()
            .insert("content-length", self.sizes[index].into());
        for (name, value) in METADATA {
            response.headers_mut().insert(name, value.parse().unwrap());
        }
        response
    }

    async fn read(
        &self,
        index: usize,
        destination: bool,
        request: &Request<Buffer>,
    ) -> Response<Buffer> {
        if destination {
            assert!(self.state.lock().unwrap().objects[index].completed);
        } else {
            if matches!(self.fault, Fault::SourceChanged) {
                return Self::response(StatusCode::PRECONDITION_FAILED, "");
            }
            if matches!(self.fault, Fault::Throttled)
                && self.state.lock().unwrap().failed_reads == 0
            {
                self.state.lock().unwrap().failed_reads += 1;
                self.read_started.notify_one();
                return Self::response(StatusCode::TOO_MANY_REQUESTS, "");
            }
            if matches!(self.fault, Fault::TimeoutRead) {
                self.state.lock().unwrap().failed_reads += 1;
                std::future::pending::<()>().await;
            }
        }
        if !destination && matches!(self.fault, Fault::BlockRead) {
            self.read_started.notify_one();
            std::future::pending::<()>().await;
        }
        assert_eq!(request.headers()["if-match"], ETAG);
        let range: BytesRange = request.headers()["range"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(!range.is_suffix());
        let size = range.size().expect("expected a bounded range");
        assert!(size > 0 && size <= BUFFER_SIZE as u64);
        assert!(range.offset() + size <= self.sizes[index] as u64);
        let range = range.to_content_range(self.sizes[index]).unwrap();
        if !destination {
            self.state.lock().unwrap().objects[index].reads += 1;
        }
        let mut bytes: Vec<u8> = range
            .clone()
            .map(|offset| ((index + offset) % 251) as u8)
            .collect();
        if destination && matches!(self.fault, Fault::CorruptDestination) {
            bytes[0] ^= 1;
        }
        let mut response = Self::response(StatusCode::PARTIAL_CONTENT, bytes);
        let content_range = BytesContentRange::default()
            .with_range(range.start as u64, (range.end - 1) as u64)
            .with_size(self.sizes[index] as u64);
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
        if matches!(self.fault, Fault::UnavailableDestination)
            || matches!(self.fault, Fault::Throttled) && state.failed_writes == 0
        {
            state.failed_writes += 1;
            return Self::response(StatusCode::SERVICE_UNAVAILABLE, "");
        }
        let object = &mut state.objects[index];
        if let Some(part) = part {
            assert!(!object.completed);
            assert_eq!(part, object.parts + 1);
            object.parts += 1;
        } else {
            Self::check_metadata(request);
            if let Some(response) = Self::collision(request, object) {
                return response;
            }
            object.completed = true;
        }
        assert!(request.body().len() <= BUFFER_SIZE);
        for (offset, byte) in request.body().to_bytes().iter().enumerate() {
            assert_eq!(*byte, ((index + object.written + offset) % 251) as u8);
        }
        object.written += request.body().len();
        if part.is_none() {
            assert_eq!(object.written, self.sizes[index]);
        }
        Self::response(StatusCode::OK, "")
    }

    fn complete(&self, index: usize, request: &Request<Buffer>) -> Response<Buffer> {
        let mut state = self.state.lock().unwrap();
        let object = &mut state.objects[index];
        if object.completed {
            return Self::response(
                StatusCode::NOT_FOUND,
                "<Error><Code>NoSuchUpload</Code></Error>",
            );
        }
        if let Some(response) = Self::collision(request, object) {
            return response;
        }
        assert_eq!(object.written, self.sizes[index]);
        assert_eq!(object.parts, self.sizes[index].div_ceil(BUFFER_SIZE));
        assert!(!object.completed);
        object.completed = true;
        Self::response(
            StatusCode::OK,
            "<CompleteMultipartUploadResult><ETag>test-etag</ETag></CompleteMultipartUploadResult>",
        )
    }

    fn check_metadata(request: &Request<Buffer>) {
        for (name, value) in METADATA {
            assert_eq!(request.headers()[name], value);
        }
    }

    fn collision(request: &Request<Buffer>, object: &ObjectState) -> Option<Response<Buffer>> {
        assert_eq!(request.headers()["if-none-match"], "*");
        (object.existing.is_some() || object.completed).then(|| {
            Self::response(
                StatusCode::PRECONDITION_FAILED,
                "<Error><Code>PreconditionFailed</Code><Message>already exists</Message></Error>",
            )
        })
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

impl HttpTransport for TestBucket {
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
        // Assert every data request stays inside its configured directory.
        let scoped_path = if request.uri().path() == "/source" {
            "/source".to_owned()
        } else if let Some(relative) = request
            .uri()
            .path()
            .strip_prefix(&format!("/source/{}", self.source_prefix))
        {
            format!("/source/{relative}")
        } else if let Some(relative) = request
            .uri()
            .path()
            .strip_prefix(&format!("/destination/{}", self.destination_prefix))
        {
            format!("/destination/{relative}")
        } else {
            panic!("request outside directory scope: {}", request.uri());
        };
        let path: Vec<_> = scoped_path.split('/').collect();
        let response = match (request.method(), path.as_slice(), params.as_slice()) {
            (&Method::GET, ["", "source"], params) => self.list(params),
            (&Method::HEAD, ["", "source", "objects", index], []) => {
                self.stat(index.parse().unwrap(), false, &request)
            }
            (&Method::GET, ["", "source", "objects", index], []) => {
                self.read(index.parse().unwrap(), false, &request).await
            }
            (&Method::HEAD, ["", "destination", "objects", index], []) => {
                self.stat(index.parse().unwrap(), true, &request)
            }
            (&Method::GET, ["", "destination", "objects", index], []) => {
                self.read(index.parse().unwrap(), true, &request).await
            }
            (&Method::PUT, ["", "destination", "objects", index], []) => {
                self.write(index.parse().unwrap(), &request, None)
            }
            (&Method::POST, ["", "destination", "objects", _], [("uploads", "")]) => {
                Self::check_metadata(&request);
                Self::response(
                    StatusCode::OK,
                    "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
                )
            }
            (
                &Method::PUT,
                ["", "destination", "objects", index],
                [("partNumber", part), ("uploadId", "test-upload")],
            ) => {
                if matches!(self.fault, Fault::AbortHang | Fault::AbortDenied) {
                    self.read_started.notify_one();
                    std::future::pending::<()>().await;
                }
                self.write(
                    index.parse().unwrap(),
                    &request,
                    Some(part.parse().unwrap()),
                )
            }
            (
                &Method::POST,
                ["", "destination", "objects", index],
                [("uploadId", "test-upload")],
            ) => self.complete(index.parse().unwrap(), &request),
            (
                &Method::DELETE,
                ["", "destination", "objects", index],
                [("uploadId", "test-upload")],
            ) => {
                self.state.lock().unwrap().aborts += 1;
                if matches!(self.fault, Fault::AbortHang) {
                    std::future::pending::<()>().await;
                }
                if matches!(self.fault, Fault::AbortDenied) {
                    Self::response(StatusCode::FORBIDDEN, "")
                } else {
                    self.state.lock().unwrap().objects[index.parse::<usize>().unwrap()].aborted =
                        true;
                    Self::response(StatusCode::NO_CONTENT, "")
                }
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
