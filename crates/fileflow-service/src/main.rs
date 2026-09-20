use fileflow_catalog::Catalog;
use fileflow_core::Config;
use fileflow_service::{serve, FileFlowService};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env();
    std::fs::create_dir_all(&cfg.data_home)?;
    tracing::info!(
        data_home = %cfg.data_home.display(),
        socket = %cfg.socket.display(),
        catalog = %cfg.catalog_path().display(),
        "starting FileFlowService"
    );

    let catalog = Catalog::open(&cfg.catalog_path())?;
    let service = FileFlowService::spawn(catalog);
    serve(&cfg.socket, service).await
}
