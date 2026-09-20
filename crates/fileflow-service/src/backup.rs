//! `.ffbackup` ZIP packages: catalog snapshot, SQL export, optional vault objects.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fileflow_catalog::CatalogError;
use fileflow_core::{
    hash_file, hash_reader, sha256_hex, vault_object_path, BackupComponent, BackupCreateReport,
    BackupManifest, BackupVerifyReport, HASH_BUFFER_SIZE, VERSION,
};
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

const SNAPSHOT_NAME: &str = "fileflow.db.snapshot";
const SQL_NAME: &str = "fileflow.sql";
const MANIFEST_NAME: &str = "manifest.json";
const CHECKSUMS_NAME: &str = "checksums.sha256";

pub fn resolve_backup_path(dest: &Path) -> Result<PathBuf, CatalogError> {
    let lossy = dest.to_string_lossy();
    if lossy.ends_with(".ffbackup") {
        if let Some(parent) = dest.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(CatalogError::msg(format!(
                    "backup destination directory does not exist: {}",
                    parent.display()
                )));
            }
        }
        return Ok(dest.to_path_buf());
    }
    if dest.exists() && dest.is_file() {
        return Err(CatalogError::msg(format!(
            "backup destination is a file, not a directory or .ffbackup path: {}",
            dest.display()
        )));
    }
    if !dest.exists() {
        fs::create_dir_all(dest).map_err(|e| {
            CatalogError::msg(format!(
                "cannot create backup destination {}: {e}",
                dest.display()
            ))
        })?;
    }
    if !dest.is_dir() {
        return Err(CatalogError::msg(format!(
            "backup destination is not a directory: {}",
            dest.display()
        )));
    }
    Ok(dest.join(timestamped_name(SystemTime::now())))
}

pub fn timestamped_name(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
        .max(0) as u64;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;
    let (year, month, day) = civil_from_days(days as i64);
    format!("FileFlow-Recovery-{year:04}{month:02}{day:02}-{hour:02}{min:02}{sec:02}.ffbackup")
}

pub fn pack_backup(
    staging: &Path,
    dest: &Path,
    source_data_home: &Path,
    include_vault: bool,
    vault_objects: u64,
    notes: Vec<String>,
) -> Result<BackupCreateReport, CatalogError> {
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
    }
    let tmp = dest.with_extension("ffbackup.partial");
    let file = File::create(&tmp).map_err(|e| CatalogError::msg(e.to_string()))?;
    let mut zip = ZipWriter::new(file);
    let opts = FileOptions::default().compression_method(CompressionMethod::Deflated);

    let mut members: Vec<(String, PathBuf)> = vec![
        (SNAPSHOT_NAME.into(), staging.join(SNAPSHOT_NAME)),
        (SQL_NAME.into(), staging.join(SQL_NAME)),
    ];
    let vault_staging = staging.join("vault");
    if include_vault && vault_staging.exists() {
        collect_files(&vault_staging, "vault", &mut members);
    }

    let mut checksums = String::new();
    let mut components = Vec::new();
    for (name, path) in &members {
        let (digest, size) = hash_file(path).map_err(|e| CatalogError::msg(e.to_string()))?;
        checksums.push_str(&format!("{digest}  {name}\n"));
        components.push(BackupComponent {
            name: name.clone(),
            size,
        });
        zip.start_file(name, opts)
            .map_err(|e| CatalogError::msg(e.to_string()))?;
        copy_file_to_zip(&mut zip, path)?;
    }

    let mut manifest = BackupManifest::new(
        iso_utc(SystemTime::now()),
        source_data_home.display().to_string(),
        include_vault,
        components.clone(),
        notes.clone(),
    );
    manifest.fileflow_version = VERSION.to_string();

    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|e| CatalogError::msg(e.to_string()))?;
    checksums.push_str(&format!(
        "{}  {MANIFEST_NAME}\n",
        sha256_hex(&manifest_bytes)
    ));
    components.push(BackupComponent {
        name: MANIFEST_NAME.into(),
        size: manifest_bytes.len() as u64,
    });
    manifest.components = components.clone();
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|e| CatalogError::msg(e.to_string()))?;
    // checksum of final manifest
    let checksums = {
        let mut lines: Vec<String> = checksums
            .lines()
            .filter(|l| !l.ends_with(MANIFEST_NAME))
            .map(|s| s.to_string())
            .collect();
        lines.push(format!("{}  {MANIFEST_NAME}", sha256_hex(&manifest_bytes)));
        lines.sort();
        let mut s = lines.join("\n");
        s.push('\n');
        s
    };

    zip.start_file(MANIFEST_NAME, opts)
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    zip.write_all(&manifest_bytes)
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    zip.start_file(CHECKSUMS_NAME, opts)
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    zip.write_all(checksums.as_bytes())
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    zip.finish().map_err(|e| CatalogError::msg(e.to_string()))?;
    fs::rename(&tmp, dest).map_err(|e| CatalogError::msg(e.to_string()))?;
    let bytes = fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
    Ok(BackupCreateReport {
        path: dest.display().to_string(),
        include_vault,
        bytes,
        vault_objects,
        notes,
    })
}

pub fn verify_backup(path: &Path) -> BackupVerifyReport {
    let mut failures = Vec::new();
    if !path.is_file() {
        return BackupVerifyReport {
            ok: false,
            path: path.display().to_string(),
            failures: vec!["backup file not found".into()],
            manifest: None,
        };
    }
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            return BackupVerifyReport {
                ok: false,
                path: path.display().to_string(),
                failures: vec![e.to_string()],
                manifest: None,
            };
        }
    };
    let mut zip = match ZipArchive::new(file) {
        Ok(z) => z,
        Err(e) => {
            return BackupVerifyReport {
                ok: false,
                path: path.display().to_string(),
                failures: vec![format!("not a zip/.ffbackup: {e}")],
                manifest: None,
            };
        }
    };

    for required in [SNAPSHOT_NAME, SQL_NAME, MANIFEST_NAME, CHECKSUMS_NAME] {
        if zip.by_name(required).is_err() {
            failures.push(format!("missing member {required}"));
        }
    }

    let checksums = match read_zip_string(&mut zip, CHECKSUMS_NAME) {
        Ok(s) => s,
        Err(e) => {
            failures.push(e);
            String::new()
        }
    };
    let expected = parse_checksums(&checksums);
    let names: Vec<String> = zip.file_names().map(|s| s.to_string()).collect();
    for name in &names {
        if name.ends_with('/') || name == CHECKSUMS_NAME {
            continue;
        }
        match zip.by_name(name) {
            Ok(mut f) => match hash_reader(&mut f) {
                Ok((digest, _)) => {
                    if let Some(exp) = expected.get(name.as_str()) {
                        if exp != &digest {
                            failures.push(format!("checksum mismatch: {name}"));
                        }
                    } else {
                        failures.push(format!("member not listed in checksums.sha256: {name}"));
                    }
                }
                Err(e) => failures.push(format!("hash {name}: {e}")),
            },
            Err(e) => failures.push(format!("read {name}: {e}")),
        }
    }
    for key in expected.keys() {
        if !names.iter().any(|n| n == key) {
            failures.push(format!("checksums lists missing member {key}"));
        }
    }

    let manifest = match read_zip_string(&mut zip, MANIFEST_NAME) {
        Ok(s) => match serde_json::from_str::<BackupManifest>(&s) {
            Ok(m) => {
                if m.format != "ffbackup" {
                    failures.push("manifest format is not ffbackup".into());
                }
                Some(m)
            }
            Err(e) => {
                failures.push(format!("manifest.json: {e}"));
                None
            }
        },
        Err(e) => {
            failures.push(e);
            None
        }
    };

    match extract_member_to_temp(&mut zip, SNAPSHOT_NAME) {
        Ok(tmp) => {
            match fileflow_catalog::Catalog::open_readonly(&tmp) {
                Ok(cat) => {
                    if !cat.integrity_ok().unwrap_or(false) {
                        failures.push("snapshot integrity_check failed".into());
                    }
                }
                Err(e) => failures.push(format!("snapshot is not openable SQLite: {e}")),
            }
            let _ = fs::remove_file(&tmp);
        }
        Err(e) => failures.push(e),
    }

    BackupVerifyReport {
        ok: failures.is_empty(),
        path: path.display().to_string(),
        failures,
        manifest,
    }
}

pub fn extract_restore(path: &Path, target: &Path) -> Result<(u64, Vec<String>), CatalogError> {
    let file = File::open(path).map_err(|e| CatalogError::msg(e.to_string()))?;
    let mut zip = ZipArchive::new(file).map_err(|e| CatalogError::msg(e.to_string()))?;
    fs::create_dir_all(target).map_err(|e| CatalogError::msg(e.to_string()))?;
    let snap_dest = target.join("catalog.sqlite");
    {
        let mut src = zip
            .by_name(SNAPSHOT_NAME)
            .map_err(|e| CatalogError::msg(e.to_string()))?;
        let mut dst = File::create(&snap_dest).map_err(|e| CatalogError::msg(e.to_string()))?;
        copy_stream(&mut src, &mut dst)?;
    }
    let mut vault_n = 0u64;
    let names: Vec<String> = zip.file_names().map(|s| s.to_string()).collect();
    for name in names {
        let Some(rel) = name.strip_prefix("vault/") else {
            continue;
        };
        if name.ends_with('/') {
            continue;
        }
        let dest = target.join("vault").join(rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
        let mut src = zip
            .by_name(&name)
            .map_err(|e| CatalogError::msg(e.to_string()))?;
        let mut dst = File::create(&dest).map_err(|e| CatalogError::msg(e.to_string()))?;
        copy_stream(&mut src, &mut dst)?;
        vault_n += 1;
    }
    Ok((
        vault_n,
        vec![format!("restored catalog to {}", snap_dest.display())],
    ))
}

pub fn copy_vault_objects(
    vault_root: &Path,
    hashes: &[String],
    staging_vault: &Path,
    notes: &mut Vec<String>,
) -> u64 {
    let mut n = 0u64;
    for sha in hashes {
        let src = vault_object_path(vault_root, sha);
        if !src.is_file() {
            notes.push(format!("vault object missing on disk: {sha}"));
            continue;
        }
        let dest = vault_object_path(staging_vault, sha);
        if let Some(parent) = dest.parent() {
            let _ = fs::create_dir_all(parent);
        }
        match fs::copy(&src, &dest) {
            Ok(_) => n += 1,
            Err(e) => notes.push(format!("copy {sha}: {e}")),
        }
    }
    n
}

pub fn target_has_user_state(target: &Path) -> bool {
    let db = target.join("catalog.sqlite");
    if db.is_file() {
        if let Ok(cat) = fileflow_catalog::Catalog::open_readonly(&db) {
            if cat.logical_file_count().unwrap_or(0) > 0 {
                return true;
            }
        }
    }
    let vault = target.join("vault/objects");
    if vault.is_dir() {
        if walkdir::WalkDir::new(&vault)
            .into_iter()
            .filter_map(|e| e.ok())
            .any(|e| e.file_type().is_file())
        {
            return true;
        }
    }
    false
}

fn collect_files(root: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) {
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let rel = entry.path().strip_prefix(root).unwrap_or(entry.path());
        let name = format!("{prefix}/{}", rel.to_string_lossy().replace('\\', "/"));
        out.push((name, entry.path().to_path_buf()));
    }
}

fn copy_file_to_zip<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    path: &Path,
) -> Result<(), CatalogError> {
    let mut input = File::open(path).map_err(|e| CatalogError::msg(e.to_string()))?;
    copy_stream(&mut input, zip)
}

fn copy_stream(src: &mut impl Read, dst: &mut impl Write) -> Result<(), CatalogError> {
    let mut buf = vec![0u8; HASH_BUFFER_SIZE];
    loop {
        let n = src
            .read(&mut buf)
            .map_err(|e| CatalogError::msg(e.to_string()))?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])
            .map_err(|e| CatalogError::msg(e.to_string()))?;
    }
    Ok(())
}

fn read_zip_string(zip: &mut ZipArchive<File>, name: &str) -> Result<String, String> {
    let mut f = zip.by_name(name).map_err(|e| e.to_string())?;
    let mut s = String::new();
    f.read_to_string(&mut s).map_err(|e| e.to_string())?;
    Ok(s)
}

fn parse_checksums(text: &str) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(hash) = parts.next() else { continue };
        let Some(name) = parts.next() else { continue };
        map.insert(name.to_string(), hash.to_lowercase());
    }
    map
}

fn extract_member_to_temp(zip: &mut ZipArchive<File>, name: &str) -> Result<PathBuf, String> {
    let mut f = zip.by_name(name).map_err(|e| e.to_string())?;
    let tmp = std::env::temp_dir().join(format!("ff-verify-{}-{name}", std::process::id()));
    let mut out = File::create(&tmp).map_err(|e| e.to_string())?;
    copy_stream(&mut f, &mut out).map_err(|e| e.to_string())?;
    Ok(tmp)
}

fn iso_utc(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
        .max(0) as u64;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}
