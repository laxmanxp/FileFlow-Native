//! Backup create / verify / restore of catalog + vault.

#![cfg(unix)]

use std::io::{Read, Write};
use std::time::Duration;

use fileflow_catalog::Catalog;
use fileflow_client::FileFlowClient;
use fileflow_core::vault_object_path;
use fileflow_rpc::connect;
use fileflow_service::{serve, FileFlowService};
use tempfile::tempdir;
use zip::write::FileOptions;
use zip::{ZipArchive, ZipWriter};

#[tokio::test]
async fn backup_verify_restore_and_tamper() {
    let dir = tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let sock = dir.path().join("fileflow.sock");
    let catalog = Catalog::open(&home.join("catalog.sqlite")).unwrap();
    let service = FileFlowService::spawn(catalog, home.clone());
    let sock_serve = sock.clone();
    tokio::spawn(async move {
        let _ = serve(&sock_serve, service).await;
    });
    let mut client = wait_for_client(&sock).await;

    let docs = home.join("docs");
    std::fs::create_dir_all(&docs).unwrap();
    let path = docs.join("notes.txt");
    std::fs::write(&path, b"backup-me").unwrap();
    let report = client.index_folder(docs.to_str().unwrap()).await.unwrap();
    let id = report.last_logical_file_id.expect("id");
    client.add_to_vault(id).await.unwrap();
    let view = client.get_file(id).await.unwrap().unwrap();
    let sha = view.sha256.clone().expect("sha");
    assert!(vault_object_path(&home.join("vault"), &sha).is_file());

    let dest_dir = dir.path().join("backups");
    std::fs::create_dir_all(&dest_dir).unwrap();
    let created = client
        .create_backup(dest_dir.to_str().unwrap(), Some(true))
        .await
        .unwrap();
    assert!(created.path.ends_with(".ffbackup"));
    assert!(created.include_vault);
    assert!(created.vault_objects >= 1);
    let backup = std::path::PathBuf::from(&created.path);
    assert!(backup.is_file());

    let verified = client
        .verify_backup(backup.to_str().unwrap())
        .await
        .unwrap();
    assert!(verified.ok, "{:?}", verified.failures);
    assert!(verified.manifest.is_some());

    let err = client
        .create_backup("/no/such/fileflow-backup-dir/out.ffbackup", Some(true))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("does not exist") || err.to_string().contains("destination"),
        "{err}"
    );

    let tampered = dir.path().join("tampered.ffbackup");
    corrupt_checksums(&backup, &tampered);
    let bad = client
        .verify_backup(tampered.to_str().unwrap())
        .await
        .unwrap();
    assert!(!bad.ok);
    assert!(!bad.failures.is_empty());

    let err = client
        .restore_backup(backup.to_str().unwrap(), None, false, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("confirm"), "{err}");

    let err = client
        .restore_backup(backup.to_str().unwrap(), None, true, None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("force") || err.to_string().contains("already has"),
        "{err}"
    );

    let home2 = dir.path().join("home2");
    std::fs::create_dir_all(&home2).unwrap();
    let sock2 = dir.path().join("fileflow2.sock");
    let catalog2 = Catalog::open(&home2.join("catalog.sqlite")).unwrap();
    let service2 = FileFlowService::spawn(catalog2, home2.clone());
    let sock2_serve = sock2.clone();
    tokio::spawn(async move {
        let _ = serve(&sock2_serve, service2).await;
    });
    let mut client2 = wait_for_client(&sock2).await;
    let restored = client2
        .restore_backup(backup.to_str().unwrap(), None, true, None)
        .await
        .unwrap();
    assert!(restored.ok);
    assert!(restored.restored_vault_objects >= 1);

    let (status, _) = client2.health().await.unwrap();
    assert_eq!(status, "ok");
    assert_eq!(
        client2.resolve_path(path.to_str().unwrap()).await.unwrap(),
        Some(id)
    );
    let view2 = client2.get_file(id).await.unwrap().unwrap();
    assert_eq!(view2.logical_file_id, id);
    assert!(view2.vaulted);
    assert_eq!(view2.sha256.as_deref(), Some(sha.as_str()));
    assert!(vault_object_path(&home2.join("vault"), &sha).is_file());
    let locs = client2.list_indexed_locations().await.unwrap();
    assert!(!locs.is_empty());
}

fn corrupt_checksums(src: &std::path::Path, dest: &std::path::Path) {
    let file = std::fs::File::open(src).unwrap();
    let mut zin = ZipArchive::new(file).unwrap();
    let out = std::fs::File::create(dest).unwrap();
    let mut zout = ZipWriter::new(out);
    let opts = FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
    for i in 0..zin.len() {
        let mut entry = zin.by_index(i).unwrap();
        let name = entry.name().to_string();
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf).unwrap();
        if name == "checksums.sha256" && !buf.is_empty() {
            buf[0] = if buf[0] == b'0' { b'1' } else { b'0' };
        }
        zout.start_file(&name, opts).unwrap();
        zout.write_all(&buf).unwrap();
    }
    zout.finish().unwrap();
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
