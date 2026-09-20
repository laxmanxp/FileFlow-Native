use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Message(String),
}

impl CatalogError {
    pub fn msg(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }
}
