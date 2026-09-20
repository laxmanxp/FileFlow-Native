//! Length-prefixed JSON RPC with a swappable byte transport.

mod protocol;
mod transport;

pub use protocol::{IndexReport, Request, Response, WatcherStatus};
pub use transport::{connect, listen, Listener, RpcConnection, TransportError};
