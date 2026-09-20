use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::LogicalFileId;

/// Directory names excluded when “exclude common build/VCS dirs” is on.
/// Matched as a **path segment** (the directory name), not a substring of a filename.
pub const DEFAULT_BUILD_VCS_DIR_NAMES: &[&str] = &[
    ".git",
    "node_modules",
    "vendor",
    "target",
    "build",
    "dist",
    ".cache",
];

pub fn default_exclude_patterns() -> Vec<String> {
    DEFAULT_BUILD_VCS_DIR_NAMES
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

/// True if any path component equals one of `names` (case-insensitive on Windows).
pub fn path_has_excluded_segment(path: &str, names: &[String]) -> bool {
    if names.is_empty() {
        return false;
    }
    Path::new(path).components().any(|c| match c {
        Component::Normal(seg) => {
            let seg = seg.to_string_lossy();
            names.iter().any(|n| segment_eq(&seg, n))
        }
        _ => false,
    })
}

fn segment_eq(seg: &str, name: &str) -> bool {
    #[cfg(windows)]
    {
        seg.eq_ignore_ascii_case(name)
    }
    #[cfg(not(windows))]
    {
        seg == name
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateMember {
    pub logical_file_id: LogicalFileId,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateGroup {
    pub sha256: String,
    pub size: u64,
    pub members: Vec<DuplicateMember>,
    /// `size * (members.len() - 1)` — extra copies.
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateScan {
    pub groups: Vec<DuplicateGroup>,
    pub group_count: u64,
    pub member_count: u64,
    pub reclaimable_bytes: u64,
    pub exclude_patterns_applied: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DuplicateQuery {
    pub min_group_size: u32,
    pub path_prefix: Option<String>,
    pub exclude_patterns: Vec<String>,
    pub min_size: u64,
    pub include_missing: bool,
}

impl DuplicateQuery {
    pub fn resolved(
        min_group_size: Option<u32>,
        path_prefix: Option<String>,
        extra_patterns: Option<Vec<String>>,
        exclude_common_build_vcs_dirs: Option<bool>,
        min_size: Option<u64>,
        include_missing: Option<bool>,
    ) -> Self {
        let mut exclude_patterns = Vec::new();
        if exclude_common_build_vcs_dirs.unwrap_or(true) {
            exclude_patterns.extend(default_exclude_patterns());
        }
        if let Some(extra) = extra_patterns {
            for p in extra {
                if !p.is_empty() && !exclude_patterns.iter().any(|e| e == &p) {
                    exclude_patterns.push(p);
                }
            }
        }
        Self {
            min_group_size: min_group_size.unwrap_or(2).max(2),
            path_prefix,
            exclude_patterns,
            min_size: min_size.unwrap_or(0),
            include_missing: include_missing.unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DuplicateActionReport {
    pub kept_logical_file_id: Option<LogicalFileId>,
    pub deleted: Vec<DuplicateMember>,
    pub used_trash: bool,
    pub permanent: bool,
    pub messages: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_modules_segment_is_excluded() {
        let names = default_exclude_patterns();
        assert!(path_has_excluded_segment(
            "/proj/node_modules/pkg/index.js",
            &names
        ));
        assert!(!path_has_excluded_segment("/proj/src/index.js", &names));
        assert!(!path_has_excluded_segment("/proj/my_target/foo.rs", &names));
        assert!(path_has_excluded_segment("/proj/target/debug/app", &names));
    }
}
