//! Shared types, configuration, streaming hashing, and search parsing.

mod backup;
mod config;
mod duplicates;
mod error;
mod hash;
mod query;
mod types;

mod vault_layout;

pub use backup::{
    BackupComponent, BackupCreateReport, BackupManifest, BackupRestoreReport, BackupVerifyReport,
};

pub use config::{Config, DEFAULT_PIPE_NAME};
pub use duplicates::{
    default_exclude_patterns, path_has_excluded_segment, DuplicateActionReport, DuplicateGroup,
    DuplicateMember, DuplicateQuery, DuplicateScan, DEFAULT_BUILD_VCS_DIR_NAMES,
};
pub use error::CoreError;
pub use hash::{hash_file, hash_reader, sha256_hex, HASH_BUFFER_SIZE};
pub use query::{parse_search, SearchQuery};
pub use types::{
    IntegrityReport, LogicalFileId, LogicalFileView, RevisionInfo, SearchHit, TodoItem, VERSION,
};
pub use vault_layout::vault_object_path;

pub type Result<T> = std::result::Result<T, CoreError>;
