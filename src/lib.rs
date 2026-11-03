//! Building blocks for bucket-to-bucket transfers.
//!
//! M0 provides validated concurrency configuration and an HTTP-only OpenDAL
//! layer. Listing, object copying and the service API follow in later milestones.

pub mod config;
pub mod http_limit;

pub use config::{ConfigError, TransferLimits};
pub use http_limit::HttpConcurrencyLimitLayer;
