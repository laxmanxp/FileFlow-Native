//! IPC contract tests against FileFlowService over a Unix domain socket.

#![cfg(unix)]

use std::time::Duration;

use fileflow_catalog::Catalog;
use fileflow_client::FileFlowClient;
use fileflow_rpc::connect;
use fileflow_service::{serve, FileFlowService};
use tempfile::tempdir;

#[tokio::test]
async fn health_resolve_metadata_and_identity_over_uds() {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("fileflow.sock");
    let db = dir.path().join("catalog.sqlite");
    let catalog = Catalog::open(&db).unwrap();
    let service = FileFlowService::spawn(catalog);
    let sock_serve = sock.clone();
    tokio::spawn(async move {
        let _ = serve(&sock_serve, service).await;
    });

    let mut client = wait_for_client(&sock).await;

    let (status, version) = client.health().await.unwrap();
    assert_eq!(status, "ok");
    assert!(!version.is_empty());

    let folder = dir.path().join("inbox");
    std::fs::create_dir_all(&folder).unwrap();
    let file_a = folder.join("notes.txt");
    std::fs::write(&file_a, b"hello fileflow").unwrap();

    let report = client.index_folder(folder.to_str().unwrap()).await.unwrap();
    assert_eq!(report.indexed, 1);
    let id = report.last_logical_file_id.expect("indexed id");

    let resolved = client
        .resolve_path(file_a.to_str().unwrap())
        .await
        .unwrap()
        .expect("resolve");
    assert_eq!(resolved, id);

    client.add_tag(id, "inbox").await.unwrap();
    client.set_note(id, "keep this").await.unwrap();
    let todo_id = client.add_todo(id, "file it").await.unwrap();
    client.set_todo_done(todo_id, true).await.unwrap();

    let hits = client
        .search("tag:inbox notes:keep todo:file ext:txt")
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].logical_file_id, id);

    let file_b = folder.join("renamed.txt");
    std::fs::rename(&file_a, &file_b).unwrap();
    client
        .update_path(id, file_b.to_str().unwrap())
        .await
        .unwrap();
    let resolved_b = client
        .resolve_path(file_b.to_str().unwrap())
        .await
        .unwrap()
        .expect("resolve after move");
    assert_eq!(resolved_b, id);

    let view = client.get_file(id).await.unwrap().unwrap();
    assert_eq!(view.logical_file_id, id);
    assert_eq!(view.tags, vec!["inbox"]);
    assert_eq!(view.note, "keep this");
    assert_eq!(view.todos.len(), 1);
    assert!(view.todos[0].done);
    assert!(view.sha256.is_some());
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
