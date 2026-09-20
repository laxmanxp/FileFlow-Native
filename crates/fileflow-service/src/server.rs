use std::path::Path;

use fileflow_rpc::{listen, Listener};
use tracing::{error, info};

use crate::FileFlowService;

pub async fn serve(socket: &Path, service: FileFlowService) -> anyhow::Result<()> {
    let listener = listen(socket).await?;
    info!(path = %socket.display(), "FileFlowService listening");
    accept_loop(listener, service).await
}

async fn accept_loop(listener: Listener, service: FileFlowService) -> anyhow::Result<()> {
    loop {
        match listener.accept().await {
            Ok(mut conn) => {
                let svc = service.clone();
                tokio::spawn(async move {
                    loop {
                        let req = match conn.recv_request().await {
                            Ok(r) => r,
                            Err(err) => {
                                tracing::debug!(error = %err, "client disconnected");
                                break;
                            }
                        };
                        let resp = svc.handle_rpc(req).await;
                        if let Err(err) = conn.send_response(&resp).await {
                            error!(error = %err, "failed to send response");
                            break;
                        }
                    }
                });
            }
            Err(err) => {
                error!(error = %err, "accept failed");
            }
        }
    }
}
