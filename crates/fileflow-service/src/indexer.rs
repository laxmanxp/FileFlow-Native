use std::path::{Path, PathBuf};

use fileflow_catalog::CatalogError;
use fileflow_rpc::IndexReport;
use walkdir::WalkDir;

use crate::handle::FileFlowService;

pub fn empty_report() -> IndexReport {
    IndexReport {
        indexed: 0,
        skipped: 0,
        last_logical_file_id: None,
    }
}

pub async fn index_one_file(
    service: &FileFlowService,
    path: &Path,
) -> Result<fileflow_core::LogicalFileId, CatalogError> {
    let path_owned = path.to_path_buf();
    let (sha256, size) = tokio::task::spawn_blocking(move || fileflow_core::hash_file(&path_owned))
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))?
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    service
        .upsert_indexed(path.to_string_lossy().into_owned(), sha256, size)
        .await
}

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

pub fn collect_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect()
}
