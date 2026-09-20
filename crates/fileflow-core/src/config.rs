use std::path::PathBuf;

/// Windows named pipe used when `FILEFLOW_SOCKET` is unset.
pub const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\FileFlow";

/// Runtime configuration. Data home and socket/pipe are overridable via env.
#[derive(Debug, Clone)]
pub struct Config {
    pub data_home: PathBuf,
    pub socket: PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        let data_home = std::env::var_os("FILEFLOW_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(default_data_home);
        let socket = std::env::var_os("FILEFLOW_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(default_socket);
        Self { data_home, socket }
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.data_home.join("catalog.sqlite")
    }
}

pub fn default_data_home() -> PathBuf {
    #[cfg(windows)]
    {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("FileFlow")
    }
    #[cfg(target_os = "macos")]
    {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("FileFlow")
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        dirs::data_dir()
            .unwrap_or_else(|| {
                let home = std::env::var_os("HOME").unwrap_or_else(|| "/tmp".into());
                PathBuf::from(home).join(".local/share")
            })
            .join("fileflow")
    }
}

pub fn default_socket() -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(DEFAULT_PIPE_NAME)
    }
    #[cfg(unix)]
    {
        if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
            return PathBuf::from(dir).join("fileflow.sock");
        }
        let user = std::env::var("USER").unwrap_or_else(|_| "user".into());
        std::env::temp_dir().join(format!("fileflow-{user}.sock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_lives_under_data_home() {
        let cfg = Config {
            data_home: PathBuf::from("/tmp/ff-test"),
            socket: PathBuf::from("/tmp/ff.sock"),
        };
        assert_eq!(
            cfg.catalog_path(),
            PathBuf::from("/tmp/ff-test/catalog.sqlite")
        );
    }
}
