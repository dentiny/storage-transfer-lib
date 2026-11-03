use std::sync::{Arc, Mutex};

use futures_util::stream;
use http::{Method, Request, Response, StatusCode};
use opendal::{
    Buffer, BytesRange, HttpBody, HttpTransport, HttpTransporter, MetadataBuilder,
    OperationContext, Operator, raw::BytesContentRange, services::S3,
};
use storage_transfer_lib::{
    CancellationToken, HttpConcurrencyLimitLayer, ObjectTask, Result, copy_object,
};

pub const BUFFER_SIZE: usize = 8 * 1024 * 1024;
const KEY: &str = "nested/object";
const SOURCE_PATH: &str = "/source/nested/object";
const DESTINATION_PATH: &str = "/destination/nested/object";
const UPLOAD_ID: &str = "test-upload";
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

    // Copy the test object through source and destination operators sharing one HTTP permit.
    pub async fn copy(&self) -> Result<()> {
        let mut metadata = MetadataBuilder::file(self.transport.size as u64);
        metadata.etag(ETAG);
        let task = ObjectTask {
            path: KEY.into(),
            metadata: metadata.build(),
        };
        copy_object(
            &self.source,
            &self.destination,
            &task,
            &CancellationToken::new(),
        )
        .await
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

    fn stat(&self, request: &Request<Buffer>) -> Response<HttpBody> {
        assert_eq!(request.headers().get("if-match").unwrap(), ETAG);
        let mut response = Self::response(StatusCode::OK, "");
        response
            .headers_mut()
            .insert("content-length", self.size.into());
        response.headers_mut().insert("etag", ETAG.parse().unwrap());
        for (name, value) in METADATA {
            response.headers_mut().insert(name, value.parse().unwrap());
        }
        response
    }

    fn read(&self, request: &Request<Buffer>, state: &mut State) -> Response<HttpBody> {
        assert_eq!(request.headers().get("if-match").unwrap(), ETAG);
        let range: BytesRange = request.headers()["range"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(!range.is_suffix());
        let size = range.size().expect("expected a bounded range");
        assert!(size > 0 && size <= BUFFER_SIZE as u64);
        assert!(range.offset() + size <= self.size as u64);
        let range = range.to_content_range(self.size).unwrap();
        state.reads += 1;
        let bytes: Vec<u8> = range.clone().map(|index| (index % 251) as u8).collect();
        let content_range = BytesContentRange::default()
            .with_range(range.start as u64, (range.end - 1) as u64)
            .with_size(self.size as u64);
        let mut response = Self::response(StatusCode::PARTIAL_CONTENT, bytes);
        response
            .headers_mut()
            .insert("content-range", content_range.to_header().parse().unwrap());
        response.headers_mut().insert("etag", ETAG.parse().unwrap());
        response
    }

    fn write(request: &Request<Buffer>, state: &mut State) -> Response<HttpBody> {
        Self::check_metadata(request);
        if let Some(response) = Self::collision(request, state) {
            return response;
        }
        Self::check_bytes(state, request.body());
        state.completed = true;
        Self::response(StatusCode::OK, "")
    }

    fn create_multipart(request: &Request<Buffer>) -> Response<HttpBody> {
        Self::check_metadata(request);
        Self::response(
            StatusCode::OK,
            "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>",
        )
    }

    fn write_part(request: &Request<Buffer>, part: usize, state: &mut State) -> Response<HttpBody> {
        assert_eq!(part, state.parts + 1);
        Self::check_bytes(state, request.body());
        state.parts += 1;
        Self::response(StatusCode::OK, "")
    }

    fn complete_multipart(
        &self,
        request: &Request<Buffer>,
        state: &mut State,
    ) -> Response<HttpBody> {
        if let Some(response) = Self::collision(request, state) {
            return response;
        }
        assert_eq!(state.written, self.size);
        state.completed = true;
        Self::response(
            StatusCode::OK,
            "<CompleteMultipartUploadResult><ETag>destination-etag</ETag></CompleteMultipartUploadResult>",
        )
    }

    fn abort_multipart(state: &mut State) -> Response<HttpBody> {
        state.aborted = true;
        Self::response(StatusCode::NO_CONTENT, "")
    }
}

impl HttpTransport for TestTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let mut query: Vec<_> =
            form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes()).collect();
        query.sort_unstable();
        let params: Vec<_> = query
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
            .collect();
        let mut state = self.state.lock().unwrap();
        let response = match (request.method(), request.uri().path(), params.as_slice()) {
            (&Method::HEAD, SOURCE_PATH, []) => self.stat(&request),
            (&Method::GET, SOURCE_PATH, []) => self.read(&request, &mut state),
            (&Method::PUT, DESTINATION_PATH, []) => Self::write(&request, &mut state),
            (&Method::POST, DESTINATION_PATH, [("uploads", "")]) => {
                Self::create_multipart(&request)
            }
            (&Method::PUT, DESTINATION_PATH, [("partNumber", part), ("uploadId", UPLOAD_ID)]) => {
                Self::write_part(&request, part.parse().unwrap(), &mut state)
            }
            (&Method::POST, DESTINATION_PATH, [("uploadId", UPLOAD_ID)]) => {
                self.complete_multipart(&request, &mut state)
            }
            (&Method::DELETE, DESTINATION_PATH, [("uploadId", UPLOAD_ID)]) => {
                Self::abort_multipart(&mut state)
            }
            _ => panic!(
                "unexpected S3 request: {} {}",
                request.method(),
                request.uri()
            ),
        };
        Ok(response)
    }
}
