use std::fs::File;
use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::Result;

/// Chunk size for streaming SHA-256. Large files are never loaded whole.
pub const HASH_BUFFER_SIZE: usize = 64 * 1024;

/// Stream SHA-256 of a file. Returns (lowercase hex digest, size in bytes).
pub fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    hash_reader(&mut file)
}

/// Stream SHA-256 of any `Read`. Large inputs are never loaded whole.
pub fn hash_reader(reader: &mut impl Read) -> Result<(String, u64)> {
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_BUFFER_SIZE];
    let mut size = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CoreError;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn streaming_hash_matches_in_memory_digest() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"fileflow-hash-fixture").unwrap();
        tmp.flush().unwrap();
        let (digest, size) = hash_file(tmp.path()).unwrap();
        assert_eq!(size, b"fileflow-hash-fixture".len() as u64);
        assert_eq!(digest, sha256_hex(b"fileflow-hash-fixture"));
        assert_eq!(digest.len(), 64);
    }

    #[test]
    fn streaming_hash_handles_larger_than_buffer() {
        let mut tmp = NamedTempFile::new().unwrap();
        let chunk = vec![0xABu8; HASH_BUFFER_SIZE + 123];
        tmp.write_all(&chunk).unwrap();
        tmp.flush().unwrap();
        let (digest, size) = hash_file(tmp.path()).unwrap();
        assert_eq!(size, chunk.len() as u64);
        assert_eq!(digest, sha256_hex(&chunk));
    }

    #[test]
    fn missing_file_is_error() {
        let err = hash_file(Path::new("/no/such/fileflow-file")).unwrap_err();
        match err {
            CoreError::Io(_) => {}
            other => panic!("expected io error, got {other}"),
        }
    }
}
