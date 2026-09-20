//! Shared types, configuration, streaming hashing, and search parsing.

mod config;
mod error;
mod hash;
mod query;
mod types;

pub use config::{Config, DEFAULT_PIPE_NAME};
pub use error::CoreError;
pub use hash::{hash_file, HASH_BUFFER_SIZE};
pub use query::{parse_search, SearchQuery};
pub use types::{LogicalFileId, LogicalFileView, SearchHit, TodoItem, VERSION};

pub type Result<T> = std::result::Result<T, CoreError>;
