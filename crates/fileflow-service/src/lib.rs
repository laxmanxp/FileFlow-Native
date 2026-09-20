//! FileFlowService: sole authority over catalog state.

mod actor;
mod handle;
mod indexer;
mod server;
mod watcher;

pub use handle::FileFlowService;
pub use server::serve;

use fileflow_core::VERSION;

pub fn health_status() -> (&'static str, &'static str) {
    ("ok", VERSION)
}
