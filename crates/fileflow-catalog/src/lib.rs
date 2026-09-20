//! SQLite catalog. Opened only by FileFlowService.

mod error;
mod schema;
mod store;

pub use error::CatalogError;
pub use store::Catalog;

pub type Result<T> = std::result::Result<T, CatalogError>;
