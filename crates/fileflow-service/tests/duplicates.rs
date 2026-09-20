//! Duplicate manager: find by SHA-256, exclusions, conservative resolve.

#![cfg(unix)]

use std::time::Duration;

use fileflow_catalog::Catalog;
use fileflow_client::FileFlowClient;
use fileflow_rpc::connect;
use fileflow_service::{serve, FileFlowService};
use tempfile::tempdir;

#[tokio::test]
async fn find_exclusions_resolve_and_refuse_delete_all() {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("fileflow.sock");
    let db = dir.path().join("catalog.sqlite");
    let vault = dir.path().join("vault");
    let catalog = Catalog::open(&db).unwrap();
    let service = FileFlowService::spawn(catalog, vault);
    let sock_serve = sock.clone();
    tokio::spawn(async move {
        let _ = serve(&sock_serve, service).await;
    });

    let mut client = wait_for_client(&sock).await;
    let root = dir.path().join("tree");
    let docs = root.join("docs");
    let nm = root.join("node_modules/pkg");
    std::fs::create_dir_all(&docs).unwrap();
    std::fs::create_dir_all(&nm).unwrap();
    let keep_path = docs.join("keep.txt");
    let drop_path = docs.join("copy.txt");
    let hidden = nm.join("copy.txt");
    let unique = docs.join("unique.txt");
    std::fs::write(&keep_path, b"identical-bytes").unwrap();
    std::fs::write(&drop_path, b"identical-bytes").unwrap();
    std::fs::write(&hidden, b"identical-bytes").unwrap();
    std::fs::write(&unique, b"only-once").unwrap();

    client.index_folder(root.to_str().unwrap()).await.unwrap();

    let open = client
        .find_duplicates(None, None, None, Some(false), None, None)
        .await
        .unwrap();
    assert_eq!(open.group_count, 1);
    assert_eq!(open.groups[0].members.len(), 3);

    let filtered = client
        .find_duplicates(None, None, None, Some(true), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.group_count, 1);
    assert_eq!(filtered.groups[0].members.len(), 2);
    assert!(filtered.groups[0]
        .members
        .iter()
        .all(|m| !m.path.contains("node_modules")));
    assert!(filtered
        .exclude_patterns_applied
        .iter()
        .any(|p| p == "node_modules"));

    let sha = filtered.groups[0].sha256.clone();
    let keep_id = filtered.groups[0]
        .members
        .iter()
        .find(|m| m.path.ends_with("keep.txt"))
        .unwrap()
        .logical_file_id;
    let drop_id = filtered.groups[0]
        .members
        .iter()
        .find(|m| m.path.ends_with("copy.txt") && m.path.contains("docs"))
        .unwrap()
        .logical_file_id;

    client.add_to_vault(keep_id).await.unwrap();
    let before = client.get_file(keep_id).await.unwrap().unwrap();
    let revs_before = client.list_revisions(keep_id).await.unwrap();
    assert!(before.vaulted);

    let err = client
        .resolve_duplicate_group(&sha, None, vec![keep_id, drop_id], true, true, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("every copy") || err.to_string().contains("allow_delete_all"),
        "{err}"
    );
    assert!(drop_path.is_file(), "refused delete-all must leave files");

    let err = client
        .resolve_duplicate_group(&sha, Some(keep_id), vec![drop_id], false, true, false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("confirm"), "{err}");

    let report = client
        .resolve_duplicate_group(&sha, Some(keep_id), vec![drop_id], true, true, false)
        .await
        .unwrap();
    assert_eq!(report.kept_logical_file_id, Some(keep_id));
    assert_eq!(report.deleted.len(), 1);
    assert!(!drop_path.exists());
    assert!(keep_path.is_file());
    assert_eq!(std::fs::read(&keep_path).unwrap(), b"identical-bytes");

    assert_eq!(
        client
            .resolve_path(drop_path.to_str().unwrap())
            .await
            .unwrap(),
        None
    );
    let after = client.get_file(keep_id).await.unwrap().unwrap();
    assert_eq!(after.logical_file_id, keep_id);
    assert!(after.vaulted);
    let revs_after = client.list_revisions(keep_id).await.unwrap();
    assert_eq!(revs_after.len(), revs_before.len());

    let last_a = docs.join("solo-a.bin");
    let last_b = docs.join("solo-b.bin");
    std::fs::write(&last_a, b"solo-pair-bytes").unwrap();
    std::fs::write(&last_b, b"solo-pair-bytes").unwrap();
    client.index_folder(root.to_str().unwrap()).await.unwrap();

    let pairs = client
        .find_duplicates(None, None, None, Some(true), None, None)
        .await
        .unwrap();
    let solo = pairs
        .groups
        .iter()
        .find(|g| g.members.iter().any(|m| m.path.ends_with("solo-a.bin")))
        .expect("solo pair");
    let solo_sha = solo.sha256.clone();
    let keep_solo = solo
        .members
        .iter()
        .find(|m| m.path.ends_with("solo-a.bin"))
        .unwrap()
        .logical_file_id;
    let drop_solo = solo
        .members
        .iter()
        .find(|m| m.path.ends_with("solo-b.bin"))
        .unwrap()
        .logical_file_id;
    client
        .resolve_duplicate_group(
            &solo_sha,
            Some(keep_solo),
            vec![drop_solo],
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert!(!last_b.exists());
    let err = client
        .delete_duplicate_member(keep_solo, true, true, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("last remaining") || err.to_string().contains("last"),
        "{err}"
    );
    assert!(last_a.is_file());
}

async fn wait_for_client(sock: &std::path::Path) -> FileFlowClient<tokio::net::UnixStream> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match connect(sock).await {
            Ok(conn) => return FileFlowClient::new(conn),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(err) => panic!("service did not start: {err}"),
        }
    }
}
