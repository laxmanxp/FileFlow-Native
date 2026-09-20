//! Best-effort trash; permanent unlink only when the caller confirms.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use fileflow_catalog::CatalogError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalKind {
    Trashed,
    Permanent,
}

pub fn remove_path(path: &Path, confirm_permanent: bool) -> Result<RemovalKind, CatalogError> {
    if !path.exists() {
        return Ok(RemovalKind::Permanent);
    }
    match try_trash(path) {
        Ok(()) => Ok(RemovalKind::Trashed),
        Err(err) if confirm_permanent => {
            if path.is_dir() {
                return Err(CatalogError::msg(
                    "duplicate delete only supports files, not directories",
                ));
            }
            fs::remove_file(path).map_err(|e| CatalogError::msg(e.to_string()))?;
            let _ = err;
            Ok(RemovalKind::Permanent)
        }
        Err(err) => Err(CatalogError::msg(format!(
            "could not move to trash ({err}); retry with confirm_permanent=true for a permanent delete"
        ))),
    }
}

fn try_trash(path: &Path) -> Result<(), String> {
    #[cfg(windows)]
    {
        windows_recycle(path)
    }
    #[cfg(target_os = "macos")]
    {
        macos_trash(path)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        xdg_trash(path)
    }
}

#[cfg(windows)]
fn windows_recycle(path: &Path) -> Result<(), String> {
    let p = path.to_string_lossy().replace('\'', "''");
    let status = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!(
                "Add-Type -AssemblyName Microsoft.VisualBasic; [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile('{p}', 'OnlyErrorDialogs', 'SendToRecycleBin')"
            ),
        ])
        .status()
        .map_err(|e| e.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("recycle bin exit {status}"))
    }
}

#[cfg(target_os = "macos")]
fn macos_trash(path: &Path) -> Result<(), String> {
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME unset".to_string())?;
    let dest_dir = Path::new(&home).join(".Trash");
    fs::create_dir_all(&dest_dir).map_err(|e| e.to_string())?;
    let name = unique_name(&dest_dir, path);
    fs::rename(path, dest_dir.join(&name)).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn xdg_trash(path: &Path) -> Result<(), String> {
    let data = std::env::var_os("XDG_DATA_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".local/share"))
        })
        .ok_or_else(|| "no XDG_DATA_HOME/HOME".to_string())?;
    let files = data.join("Trash/files");
    let info = data.join("Trash/info");
    fs::create_dir_all(&files).map_err(|e| e.to_string())?;
    fs::create_dir_all(&info).map_err(|e| e.to_string())?;
    let orig = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let name = unique_name(&files, path);
    let dest = files.join(&name);
    match fs::rename(path, &dest) {
        Ok(()) => {}
        Err(_) => {
            fs::copy(path, &dest).map_err(|e| e.to_string())?;
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    let encoded = percent_encode_path(&orig.to_string_lossy());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let deletion = unix_to_localish(now);
    let mut inf =
        fs::File::create(info.join(format!("{name}.trashinfo"))).map_err(|e| e.to_string())?;
    write!(
        inf,
        "[Trash Info]\nPath={encoded}\nDeletionDate={deletion}\n"
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(unix)]
fn unique_name(dir: &Path, src: &Path) -> String {
    let base = src
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".into());
    if !dir.join(&base).exists() {
        return base;
    }
    for i in 1..10_000 {
        let candidate = format!("{base}.{i}");
        if !dir.join(&candidate).exists() {
            return candidate;
        }
    }
    format!("{base}.{}", std::process::id())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn percent_encode_path(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'.' | b'-' | b'_' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(all(unix, not(target_os = "macos")))]
fn unix_to_localish(secs: i64) -> String {
    // Trash spec wants local time; UTC is acceptable for v1.
    let secs = secs.max(0) as u64;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}")
}

#[cfg(all(unix, not(target_os = "macos")))]
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
