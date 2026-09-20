//! Watcher integration: create / rename / modify / delete against a temp tree.

#![cfg(unix)]

use std::time::Duration;

use fileflow_catalog::Catalog;
use fileflow_client::FileFlowClient;
use fileflow_rpc::connect;
use fileflow_service::{serve, FileFlowService};
use tempfile::tempdir;

#[tokio::test]
async fn watcher_create_rename_modify_delete() {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("fileflow.sock");
    let db = dir.path().join("catalog.sqlite");
    let catalog = Catalog::open(&db).unwrap();
    let vault = dir.path().join("vault");
    let service = FileFlowService::spawn(catalog, vault);
    let sock_serve = sock.clone();
    tokio::spawn(async move {
        let _ = serve(&sock_serve, service).await;
    });

    let mut client = wait_for_client(&sock).await;
    let folder = dir.path().join("watched");
    std::fs::create_dir_all(&folder).unwrap();
    let original = folder.join("alpha.txt");
    std::fs::write(&original, b"v1").unwrap();

    let report = client.index_folder(folder.to_str().unwrap()).await.unwrap();
    assert_eq!(report.indexed, 1);
    let id = report.last_logical_file_id.expect("id");
    client.add_tag(id, "watch").await.unwrap();
    client.set_note(id, "keep metadata").await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let st = client.watcher_status().await.unwrap();
        if !st.paused && !st.roots.is_empty() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("watcher did not start on indexed root: {st:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let created = folder.join("beta.txt");
    std::fs::write(&created, b"new file").unwrap();
    let created_id = wait_resolve(
        &mut client,
        created.to_str().unwrap(),
        Duration::from_secs(8),
    )
    .await
    .expect("created file indexed");
    assert_ne!(created_id, id);

    let renamed = folder.join("alpha-moved.txt");
    std::fs::rename(&original, &renamed).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if client
            .resolve_path(renamed.to_str().unwrap())
            .await
            .unwrap()
            == Some(id)
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("rename did not keep logical id {id}");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let view = client.get_file(id).await.unwrap().unwrap();
    assert_eq!(view.logical_file_id, id);
    assert_eq!(view.tags, vec!["watch"]);
    assert_eq!(view.note, "keep metadata");

    let old_hash = view.sha256.clone();
    std::fs::write(&renamed, b"v2-changed").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let v = client.get_file(id).await.unwrap().unwrap();
        if v.sha256 != old_hash && v.sha256.is_some() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("modify did not record a new hash (still {old_hash:?})");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }

    std::fs::remove_file(&renamed).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if client
            .resolve_path(renamed.to_str().unwrap())
            .await
            .unwrap()
            .is_none()
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("delete did not tombstone path");
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let after = client.get_file(id).await.unwrap().unwrap();
    assert!(after.paths.is_empty());
    assert_eq!(after.tags, vec!["watch"]);
    assert_eq!(after.note, "keep metadata");

    let st = client.watcher_status().await.unwrap();
    assert!(!st.roots.is_empty());
    client.pause_watcher().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if client.watcher_status().await.unwrap().paused {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("watcher did not pause");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.resume_watcher().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if !client.watcher_status().await.unwrap().paused {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("watcher did not resume");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = created_id;
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

async fn wait_resolve(
    client: &mut FileFlowClient<tokio::net::UnixStream>,
    path: &str,
    timeout: Duration,
) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(id) = client.resolve_path(path).await.unwrap() {
            return Some(id);
        }
        if tokio::time::Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
}
