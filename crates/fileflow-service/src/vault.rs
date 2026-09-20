//! Content-addressed vault blobs under `$FILEFLOW_DATA_HOME/vault`.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use fileflow_catalog::CatalogError;
use fileflow_core::{hash_file, vault_object_path, IntegrityReport, HASH_BUFFER_SIZE};

#[derive(Clone)]
pub struct VaultStore {
    root: PathBuf,
}

impl VaultStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn object_path(&self, sha256: &str) -> PathBuf {
        vault_object_path(&self.root, sha256)
    }

    pub fn contains(&self, sha256: &str) -> bool {
        self.object_path(sha256).is_file()
    }

    /// Stream-copy `src` into the CAS. No-op if the object already exists.
    pub fn put_from_file(&self, src: &Path, sha256: &str) -> Result<(), CatalogError> {
        let dest = self.object_path(sha256);
        if dest.is_file() {
            return Ok(());
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
        let tmp = dest.with_extension("tmp");
        copy_streaming(src, &tmp)?;
        let (got, _) = hash_file(&tmp).map_err(|e| CatalogError::msg(e.to_string()))?;
        if got != sha256 {
            let _ = fs::remove_file(&tmp);
            return Err(CatalogError::msg(format!(
                "vault put hash mismatch: expected {sha256}, got {got}"
            )));
        }
        fs::rename(&tmp, &dest).map_err(|e| CatalogError::msg(e.to_string()))?;
        Ok(())
    }

    pub fn restore_to(&self, sha256: &str, dest: &Path) -> Result<(), CatalogError> {
        let src = self.object_path(sha256);
        if !src.is_file() {
            return Err(CatalogError::msg(format!("vault object missing: {sha256}")));
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
        let tmp = match dest.file_name() {
            Some(name) => dest
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!(".{}.ffrestore", name.to_string_lossy())),
            None => dest.with_extension("ffrestore"),
        };
        copy_streaming(&src, &tmp)?;
        match fs::rename(&tmp, dest) {
            Ok(()) => Ok(()),
            Err(_) => {
                if dest.exists() {
                    fs::remove_file(dest).map_err(|e| CatalogError::msg(e.to_string()))?;
                }
                fs::rename(&tmp, dest).map_err(|e| {
                    let _ = fs::remove_file(&tmp);
                    CatalogError::msg(format!(
                        "restore failed (file may be locked by another app): {e}"
                    ))
                })
            }
        }
    }

    pub fn verify(&self, sha256: &str) -> IntegrityReport {
        let path = self.object_path(sha256);
        if !path.is_file() {
            return IntegrityReport {
                ok: false,
                expected_sha256: sha256.to_string(),
                actual_sha256: None,
                message: "blob not present in vault".into(),
            };
        }
        match hash_file(&path) {
            Ok((actual, _)) => IntegrityReport {
                ok: actual == sha256,
                expected_sha256: sha256.to_string(),
                actual_sha256: Some(actual.clone()),
                message: if actual == sha256 {
                    "ok".into()
                } else {
                    "hash mismatch (blob tampered or corrupt)".into()
                },
            },
            Err(err) => IntegrityReport {
                ok: false,
                expected_sha256: sha256.to_string(),
                actual_sha256: None,
                message: err.to_string(),
            },
        }
    }

    pub fn delete_object(&self, sha256: &str) {
        let path = self.object_path(sha256);
        let _ = fs::remove_file(path);
    }
}

fn copy_streaming(src: &Path, dst: &Path) -> Result<(), CatalogError> {
    let mut input = File::open(src).map_err(|e| CatalogError::msg(e.to_string()))?;
    let mut output = File::create(dst).map_err(|e| CatalogError::msg(e.to_string()))?;
    let mut buf = vec![0u8; HASH_BUFFER_SIZE];
    loop {
        let n = input
            .read(&mut buf)
            .map_err(|e| CatalogError::msg(e.to_string()))?;
        if n == 0 {
            break;
        }
        output
            .write_all(&buf[..n])
            .map_err(|e| CatalogError::msg(e.to_string()))?;
    }
    output
        .sync_all()
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    Ok(())
}
