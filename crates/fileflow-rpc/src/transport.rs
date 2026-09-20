use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Request, Response};

const MAX_FRAME: u32 = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
    #[error("unsupported transport on this platform")]
    Unsupported,
}

pub struct RpcConnection<S> {
    stream: S,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RpcConnection<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub async fn send_request(&mut self, req: &Request) -> Result<(), TransportError> {
        write_frame(&mut self.stream, req).await
    }

    pub async fn recv_request(&mut self) -> Result<Request, TransportError> {
        read_frame(&mut self.stream).await
    }

    pub async fn send_response(&mut self, resp: &Response) -> Result<(), TransportError> {
        write_frame(&mut self.stream, resp).await
    }

    pub async fn recv_response(&mut self) -> Result<Response, TransportError> {
        read_frame(&mut self.stream).await
    }

    pub async fn call(&mut self, req: &Request) -> Result<Response, TransportError> {
        self.send_request(req).await?;
        self.recv_response().await
    }
}

async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    let len = bytes.len() as u32;
    if len > MAX_FRAME {
        return Err(TransportError::FrameTooLarge(len));
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_frame<R, T>(reader: &mut R) -> Result<T, TransportError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME {
        return Err(TransportError::FrameTooLarge(len));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

#[cfg(unix)]
pub struct Listener {
    inner: tokio::net::UnixListener,
}

#[cfg(windows)]
pub struct Listener {
    pipe_name: String,
}

#[cfg(unix)]
pub async fn listen(path: &Path) -> Result<Listener, TransportError> {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let inner = tokio::net::UnixListener::bind(path)?;
    Ok(Listener { inner })
}

#[cfg(unix)]
impl Listener {
    pub async fn accept(&self) -> Result<RpcConnection<tokio::net::UnixStream>, TransportError> {
        let (stream, _) = self.inner.accept().await?;
        Ok(RpcConnection::new(stream))
    }
}

#[cfg(unix)]
pub async fn connect(path: &Path) -> Result<RpcConnection<tokio::net::UnixStream>, TransportError> {
    let stream = tokio::net::UnixStream::connect(path).await?;
    Ok(RpcConnection::new(stream))
}

#[cfg(windows)]
pub async fn listen(path: &Path) -> Result<Listener, TransportError> {
    Ok(Listener {
        pipe_name: path.to_string_lossy().into_owned(),
    })
}

#[cfg(windows)]
impl Listener {
    pub async fn accept(
        &self,
    ) -> Result<RpcConnection<tokio::net::windows::named_pipe::NamedPipeServer>, TransportError>
    {
        use tokio::net::windows::named_pipe::ServerOptions;
        let server = ServerOptions::new()
            .first_pipe_instance(false)
            .create(&self.pipe_name)?;
        server.connect().await?;
        Ok(RpcConnection::new(server))
    }
}

#[cfg(windows)]
pub async fn connect(
    path: &Path,
) -> Result<RpcConnection<tokio::net::windows::named_pipe::NamedPipeClient>, TransportError> {
    use tokio::net::windows::named_pipe::ClientOptions;
    let client = ClientOptions::new().open(path.to_string_lossy().as_ref())?;
    Ok(RpcConnection::new(client))
}
