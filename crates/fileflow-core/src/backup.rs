use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupComponent {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    pub format: String,
    pub format_version: u32,
    pub fileflow_version: String,
    pub created_at: String,
    pub source_data_home: String,
    pub include_vault: bool,
    pub components: Vec<BackupComponent>,
    pub notes: Vec<String>,
}

impl BackupManifest {
    pub fn new(
        created_at: String,
        source_data_home: String,
        include_vault: bool,
        components: Vec<BackupComponent>,
        notes: Vec<String>,
    ) -> Self {
        Self {
            format: "ffbackup".into(),
            format_version: 1,
            fileflow_version: crate::VERSION.to_string(),
            created_at,
            source_data_home,
            include_vault,
            components,
            notes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupCreateReport {
    pub path: String,
    pub include_vault: bool,
    pub bytes: u64,
    pub vault_objects: u64,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupVerifyReport {
    pub ok: bool,
    pub path: String,
    pub failures: Vec<String>,
    pub manifest: Option<BackupManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupRestoreReport {
    pub ok: bool,
    pub target_data_home: String,
    pub restored_vault_objects: u64,
    pub notes: Vec<String>,
}
