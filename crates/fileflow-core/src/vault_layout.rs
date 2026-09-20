use std::path::{Path, PathBuf};

/// Content-addressed blob path under a vault root.
/// Layout: `{vault_root}/objects/{aa}/{bb}/{sha256}` where `aa`/`bb` are the first four hex chars.
pub fn vault_object_path(vault_root: &Path, sha256: &str) -> PathBuf {
    let sha = sha256.trim().to_lowercase();
    let a = sha.get(0..2).unwrap_or("00");
    let b = sha.get(2..4).unwrap_or("00");
    vault_root.join("objects").join(a).join(b).join(&sha)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn content_addressed_layout() {
        let p = vault_object_path(
            Path::new("/data/vault"),
            "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        );
        assert_eq!(
            p,
            Path::new(
                "/data/vault/objects/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )
        );
    }
}
