use std::path::{Path, PathBuf};

use fileflow_catalog::CatalogError;
use fileflow_rpc::IndexReport;
use walkdir::WalkDir;

use crate::handle::{empty_report, index_one_file, FileFlowService};

pub async fn index_path(
    service: &FileFlowService,
    path: &str,
) -> Result<IndexReport, CatalogError> {
    let p = PathBuf::from(path);
    if p.is_dir() {
        return index_folder(service, path).await;
    }
    if !p.is_file() {
        return Err(CatalogError::msg(format!("not a file: {path}")));
    }
    let id = index_one_file(service, &p).await?;
    Ok(IndexReport {
        indexed: 1,
        skipped: 0,
        last_logical_file_id: Some(id),
    })
}

pub async fn index_folder(
    service: &FileFlowService,
    path: &str,
) -> Result<IndexReport, CatalogError> {
    let root = PathBuf::from(path);
    if !root.is_dir() {
        return Err(CatalogError::msg(format!("not a directory: {path}")));
    }
    let files = tokio::task::spawn_blocking(move || collect_files(&root))
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))?;

    let mut report = empty_report();
    for file in files {
        match index_one_file(service, &file).await {
            Ok(id) => {
                report.indexed += 1;
                report.last_logical_file_id = Some(id);
            }
            Err(err) => {
                tracing::warn!(path = %file.display(), error = %err, "skip file");
                report.skipped += 1;
            }
        }
    }
    Ok(report)
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect()
}
