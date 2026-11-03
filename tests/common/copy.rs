use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream;
use http::{Request, Response, StatusCode};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, MetadataBuilder, OperationContext, Operator,
    services::S3,
};
use storage_transfer_lib::{
    CancellationToken, HttpConcurrencyLimitLayer, ObjectTask, Result, copy_object,
};
use tokio::time::timeout;

pub const BUFFER_SIZE: usize = 8 * 1024 * 1024;
const KEY: &str = "nested/object";
const ETAG: &str = "\"source-etag\"";
const METADATA: [(&str, &str); 5] = [
    ("content-type", "application/octet-stream"),
    ("content-encoding", "identity"),
    ("content-disposition", "attachment"),
    ("cache-control", "max-age=60"),
    ("x-amz-meta-owner", "test"),
];

#[derive(Default)]
struct State {
    // Total payload bytes validated by the fake destination across PUTs and uploaded parts.
    written: usize,
    // Number of source range GET requests served by the fake transport.
    reads: usize,
    parts: usize,
    completed: bool,
    aborted: bool,
    // Existing destination data used to verify that a copy never overwrites it; None means absent.
    existing: Option<&'static [u8]>,
}

#[derive(Clone)]
struct TestTransport {
    size: usize,
    state: Arc<Mutex<State>>,
}

pub struct TestCopy {
    transport: TestTransport,
    source: Operator,
    destination: Operator,
}

impl TestCopy {
    pub fn new(size: usize, destination_exists: bool) -> Self {
        let transport = TestTransport {
            size,
            state: Arc::new(Mutex::new(State {
                existing: destination_exists.then_some(b"original destination data"),
                ..State::default()
            })),
        };
        let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
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
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(transport.clone())),
            )
            .layer(layer.clone())
        };
        Self {
            source: operator("source"),
            destination: operator("destination"),
            transport,
        }
    }

    // Bound test execution to detect a leaked permit or H=1 read/write deadlock.
    pub async fn copy(&self) -> Result<()> {
        let mut metadata = MetadataBuilder::file(self.transport.size as u64);
        metadata.etag(ETAG);
        let task = ObjectTask {
            path: KEY.into(),
            metadata: metadata.build(),
        };
        timeout(
            Duration::from_secs(10),
            copy_object(
                &self.source,
                &self.destination,
                &task,
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("copy stalled with HTTP concurrency 1")
    }

    pub fn assert_completed(&self) {
        let state = self.transport.state.lock().unwrap();
        assert!(state.completed);
        assert_eq!(state.written, self.transport.size);
        assert_eq!(state.reads, self.transport.size.div_ceil(BUFFER_SIZE));
        assert_eq!(
            state.parts,
            if self.transport.size > BUFFER_SIZE {
                self.transport.size.div_ceil(BUFFER_SIZE)
            } else {
                0
            }
        );
        assert!(!state.aborted);
    }

    pub fn assert_destination_unchanged(&self) {
        let state = self.transport.state.lock().unwrap();
        assert_eq!(
            state.existing,
            Some(b"original destination data".as_slice())
        );
        assert!(!state.completed);
        assert_eq!(state.aborted, self.transport.size > BUFFER_SIZE);
    }
}

impl TestTransport {
    fn response(status: StatusCode, body: impl Into<Buffer>) -> Response<HttpBody> {
        let body = body.into();
        let size = body.len();
        Response::builder()
            .status(status)
            .header("content-length", size)
            .header("etag", "\"destination-etag\"")
            .body(HttpBody::new(stream::iter([Ok(body)]), Some(size as u64)))
            .unwrap()
    }

    fn check_metadata(request: &Request<Buffer>) {
        for (name, value) in METADATA {
            assert_eq!(request.headers().get(name).unwrap(), value);
        }
    }

    fn check_bytes(state: &mut State, body: &Buffer) {
        assert!(body.len() <= BUFFER_SIZE);
        // Validate each uploaded range without retaining the complete object.
        for (offset, byte) in body.to_bytes().iter().enumerate() {
            assert_eq!(*byte, ((state.written + offset) % 251) as u8);
        }
        state.written += body.len();
    }

    fn collision(request: &Request<Buffer>, state: &State) -> Option<Response<HttpBody>> {
        assert_eq!(request.headers().get("if-none-match").unwrap(), "*");
        state.existing.map(|_| {
            Self::response(
                StatusCode::PRECONDITION_FAILED,
                "<Error><Code>PreconditionFailed</Code><Message>already exists</Message></Error>",
            )
        })
    }
}

impl HttpTransport for TestTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let method = request.method().as_str();
        let query = request.uri().query().unwrap_or_default();
        let mut state = self.state.lock().unwrap();
        if request.uri().path() == format!("/source/{KEY}") {
            assert_eq!(request.headers().get("if-match").unwrap(), ETAG);
            if method == "HEAD" {
                let mut response = Self::response(StatusCode::OK, "");
                response
                    .headers_mut()
                    .insert("content-length", self.size.into());
                response.headers_mut().insert("etag", ETAG.parse().unwrap());
                for (name, value) in METADATA {
                    response.headers_mut().insert(name, value.parse().unwrap());
                }
                return Ok(response);
            }
            assert_eq!(method, "GET");
            let range = request.headers().get("range").unwrap().to_str().unwrap();
            let (start, end) = range
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            let end: usize = end.parse().unwrap();
            assert!(end < self.size && end - start < BUFFER_SIZE);
            state.reads += 1;
            let bytes: Vec<u8> = (start..=end).map(|index| (index % 251) as u8).collect();
            let mut response = Self::response(StatusCode::PARTIAL_CONTENT, bytes);
            response.headers_mut().insert(
                "content-range",
                format!("bytes {start}-{end}/{}", self.size)
                    .parse()
                    .unwrap(),
            );
            response.headers_mut().insert("etag", ETAG.parse().unwrap());
            return Ok(response);
        }
        assert_eq!(request.uri().path(), format!("/destination/{KEY}"));
        if method == "POST" && query == "uploads" {
            Self::check_metadata(&request);
            return Ok(Self::response(
                StatusCode::OK,
                "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
            ));
        }
        if method == "PUT" {
            if query.is_empty() {
                Self::check_metadata(&request);
                if let Some(response) = Self::collision(&request, &state) {
                    return Ok(response);
                }
                state.completed = true;
            } else {
                assert!(query.contains("uploadId=test-upload"));
                assert!(
                    query
                        .split('&')
                        .any(|param| param == format!("partNumber={}", state.parts + 1))
                );
                state.parts += 1;
            }
            Self::check_bytes(&mut state, request.body());
            return Ok(Self::response(StatusCode::OK, ""));
        }
        assert!(query.contains("uploadId=test-upload"));
        if method == "POST" {
            if let Some(response) = Self::collision(&request, &state) {
                return Ok(response);
            }
            assert_eq!(state.written, self.size);
            state.completed = true;
            return Ok(Self::response(
                StatusCode::OK,
                "<CompleteMultipartUploadResult><ETag>destination-etag</ETag></CompleteMultipartUploadResult>",
            ));
        }
        assert_eq!(method, "DELETE");
        state.aborted = true;
        Ok(Self::response(StatusCode::NO_CONTENT, ""))
    }
}
