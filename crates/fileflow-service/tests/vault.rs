//! Vault: store blob, capture revision on modify, restore, verify, rename identity.

#![cfg(unix)]

use std::time::Duration;

use fileflow_catalog::Catalog;
use fileflow_client::FileFlowClient;
use fileflow_core::vault_object_path;
use fileflow_rpc::connect;
use fileflow_service::{serve, FileFlowService};
use tempfile::tempdir;

#[tokio::test]
async fn vault_store_revision_restore_verify_and_rename() {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("fileflow.sock");
    let db = dir.path().join("catalog.sqlite");
    let vault = dir.path().join("vault");
    let catalog = Catalog::open(&db).unwrap();
    let service = FileFlowService::spawn(catalog, vault.clone());
    let sock_serve = sock.clone();
    tokio::spawn(async move {
        let _ = serve(&sock_serve, service).await;
    });

    let mut client = wait_for_client(&sock).await;
    let folder = dir.path().join("docs");
    std::fs::create_dir_all(&folder).unwrap();
    let path = folder.join("notes.txt");
    std::fs::write(&path, b"revision-one").unwrap();

    let report = client.index_folder(folder.to_str().unwrap()).await.unwrap();
    let id = report.last_logical_file_id.expect("id");
    client.add_to_vault(id).await.unwrap();

    let view = client.get_file(id).await.unwrap().unwrap();
    assert!(view.vaulted);
    let first_sha = view.sha256.clone().expect("sha");
    let blob = vault_object_path(&vault, &first_sha);
    assert!(blob.is_file(), "AddToVault should store a blob");

    let revs = client.list_revisions(id).await.unwrap();
    assert_eq!(revs.len(), 1);
    let first_rev = revs[0].revision_id;
    let got = client.get_revision(id, first_rev).await.unwrap();
    assert_eq!(got.sha256, first_sha);
    assert!(got.is_current);

    std::fs::write(&path, b"revision-two-changed").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let revs = client.list_revisions(id).await.unwrap();
        if revs.len() >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("vaulted modify did not append a revision");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let revs = client.list_revisions(id).await.unwrap();
    let current = revs.iter().find(|r| r.is_current).unwrap();
    assert_ne!(current.sha256, first_sha);
    assert!(vault_object_path(&vault, &first_sha).is_file());
    assert!(vault_object_path(&vault, &current.sha256).is_file());

    client.set_current_revision(id, first_rev).await.unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"revision-one");
    let view = client.get_file(id).await.unwrap().unwrap();
    assert_eq!(view.current_revision_id, Some(first_rev));
    assert_eq!(view.sha256.as_deref(), Some(first_sha.as_str()));

    let moved = folder.join("notes-renamed.txt");
    std::fs::rename(&path, &moved).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if client.resolve_path(moved.to_str().unwrap()).await.unwrap() == Some(id) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("vaulted rename lost logical id");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let view = client.get_file(id).await.unwrap().unwrap();
    assert!(view.vaulted);
    assert_eq!(view.logical_file_id, id);

    let report = client.verify_revision(id, first_rev).await.unwrap();
    assert!(report.ok);

    std::fs::write(&blob, b"tampered-bytes-not-original").unwrap();
    let report = client.verify_content_object(&first_sha).await.unwrap();
    assert!(!report.ok);
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
