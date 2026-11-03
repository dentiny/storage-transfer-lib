#[path = "common/copy.rs"]
mod common;

use common::{BUFFER_SIZE, TestCopy};
use storage_transfer_lib::Error;

#[tokio::test]
async fn copies_objects_in_bounded_ranges_with_one_http_permit() {
    for size in [
        0,
        17,
        BUFFER_SIZE,
        BUFFER_SIZE + 13,
        3 * BUFFER_SIZE + 7,
        16 * BUFFER_SIZE + 31,
    ] {
        let copy = TestCopy::new(size, false);
        copy.copy().await.unwrap();
        copy.assert_completed();
    }
}

#[tokio::test]
async fn existing_destination_is_not_overwritten() {
    for size in [17, 2 * BUFFER_SIZE + 13] {
        let copy = TestCopy::new(size, true);
        assert!(
            matches!(copy.copy().await, Err(Error::Storage(error)) if error.kind() == opendal::ErrorKind::ConditionNotMatch)
        );
        copy.assert_destination_unchanged();
    }
}
