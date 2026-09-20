//! Service-owned filesystem watcher. UI never watches the disk itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fileflow_catalog::CatalogError;
use fileflow_rpc::WatcherStatus;
use notify::event::{ModifyKind, RenameMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::indexer::{collect_files, index_one_file};
use crate::FileFlowService;

const EVENT_QUEUE: usize = 256;
const PENDING_CAP: usize = 1024;
const DEBOUNCE: Duration = Duration::from_millis(200);
const RENAME_FROM_TTL: Duration = Duration::from_millis(400);

#[derive(Clone)]
pub struct WatcherHandle {
    cmd_tx: mpsc::Sender<WatcherCmd>,
    snapshot: Arc<Mutex<Snapshot>>,
}

#[derive(Clone)]
pub(crate) struct Snapshot {
    paused: bool,
    roots: Vec<String>,
    queue_depth: usize,
    last_error: Option<String>,
}

pub(crate) enum WatcherCmd {
    AddRoot(String),
    RemoveRoot(String),
    Pause,
    Resume,
}

#[derive(Debug)]
enum Pending {
    Index { at: Instant },
    Tombstone { at: Instant },
    Rename { from: PathBuf, at: Instant },
}

impl Pending {
    fn at(&self) -> Instant {
        match self {
            Pending::Index { at } | Pending::Tombstone { at } | Pending::Rename { at, .. } => *at,
        }
    }
}

impl WatcherHandle {
    pub fn pair() -> (Self, mpsc::Receiver<WatcherCmd>, Arc<Mutex<Snapshot>>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let snapshot = Arc::new(Mutex::new(Snapshot {
            paused: false,
            roots: Vec::new(),
            queue_depth: 0,
            last_error: None,
        }));
        (
            Self {
                cmd_tx,
                snapshot: snapshot.clone(),
            },
            cmd_rx,
            snapshot,
        )
    }

    pub fn start(
        service: FileFlowService,
        cmd_rx: mpsc::Receiver<WatcherCmd>,
        snapshot: Arc<Mutex<Snapshot>>,
    ) {
        tokio::spawn(async move {
            if let Err(err) = run_loop(service, cmd_rx, snapshot).await {
                tracing::error!(error = %err, "watcher loop exited");
            }
        });
    }

    pub async fn add_root(&self, path: String) {
        let _ = self.cmd_tx.send(WatcherCmd::AddRoot(path)).await;
    }

    pub async fn remove_root(&self, path: String) {
        let _ = self.cmd_tx.send(WatcherCmd::RemoveRoot(path)).await;
    }

    pub async fn pause(&self) {
        let _ = self.cmd_tx.send(WatcherCmd::Pause).await;
    }

    pub async fn resume(&self) {
        let _ = self.cmd_tx.send(WatcherCmd::Resume).await;
    }

    pub fn status(&self) -> WatcherStatus {
        let snap = self.snapshot.lock().expect("watcher status mutex");
        WatcherStatus {
            paused: snap.paused,
            roots: snap.roots.clone(),
            queue_depth: snap.queue_depth,
            last_error: snap.last_error.clone(),
        }
    }
}

async fn run_loop(
    service: FileFlowService,
    mut cmd_rx: mpsc::Receiver<WatcherCmd>,
    snapshot: Arc<Mutex<Snapshot>>,
) -> Result<(), CatalogError> {
    let (ev_tx, mut ev_rx) = mpsc::channel::<notify::Result<Event>>(EVENT_QUEUE);
    let ev_tx_for_depth = ev_tx.clone();
    let snap_cb = snapshot.clone();
    let mut watcher = RecommendedWatcher::new(
        move |res| {
            if let Ok(mut s) = snap_cb.lock() {
                s.queue_depth = EVENT_QUEUE.saturating_sub(ev_tx.capacity());
            }
            if let Err(err) = ev_tx.try_send(res) {
                if let Ok(mut s) = snap_cb.lock() {
                    s.last_error = Some(format!("watcher event queue full; dropped ({err})"));
                    s.queue_depth = EVENT_QUEUE;
                }
            }
        },
        Config::default(),
    )
    .map_err(|e| CatalogError::msg(e.to_string()))?;

    let mut pending: HashMap<PathBuf, Pending> = HashMap::new();
    let mut rename_from: Option<(PathBuf, Instant)> = None;
    let mut interval = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    WatcherCmd::AddRoot(path) => {
                        match watcher.watch(Path::new(&path), RecursiveMode::Recursive) {
                            Ok(()) => {
                                let mut s = snapshot.lock().expect("snapshot");
                                if !s.roots.iter().any(|r| r == &path) {
                                    s.roots.push(path);
                                    s.roots.sort();
                                }
                                s.last_error = None;
                            }
                            Err(err) => {
                                snapshot.lock().expect("snapshot").last_error =
                                    Some(format!("watch {path}: {err}"));
                            }
                        }
                    }
                    WatcherCmd::RemoveRoot(path) => {
                        let _ = watcher.unwatch(Path::new(&path));
                        let mut s = snapshot.lock().expect("snapshot");
                        s.roots.retain(|r| r != &path);
                    }
                    WatcherCmd::Pause => {
                        snapshot.lock().expect("snapshot").paused = true;
                        let roots = snapshot.lock().expect("snapshot").roots.clone();
                        for r in roots {
                            let _ = watcher.unwatch(Path::new(&r));
                        }
                    }
                    WatcherCmd::Resume => {
                        snapshot.lock().expect("snapshot").paused = false;
                        let roots = snapshot.lock().expect("snapshot").roots.clone();
                        for r in &roots {
                            if let Err(err) = watcher.watch(Path::new(r), RecursiveMode::Recursive) {
                                snapshot.lock().expect("snapshot").last_error =
                                    Some(format!("rewatch {r}: {err}"));
                            }
                        }
                        rescan_roots(&service, &roots).await;
                    }
                }
            }
            ev = ev_rx.recv() => {
                let Some(ev) = ev else { break; };
                {
                    let mut s = snapshot.lock().expect("snapshot");
                    s.queue_depth = EVENT_QUEUE.saturating_sub(ev_tx_for_depth.capacity());
                }
                if snapshot.lock().expect("snapshot").paused {
                    continue;
                }
                match ev {
                    Ok(event) => {
                        ingest(event, &mut pending, &mut rename_from);
                        if pending.len() > PENDING_CAP {
                            flush_due(&service, &mut pending, Duration::ZERO).await;
                        }
                    }
                    Err(err) => {
                        snapshot.lock().expect("snapshot").last_error = Some(err.to_string());
                    }
                }
            }
            _ = interval.tick() => {
                expire_rename_from(&mut rename_from, &mut pending);
                flush_due(&service, &mut pending, DEBOUNCE).await;
                let mut s = snapshot.lock().expect("snapshot");
                s.queue_depth = EVENT_QUEUE.saturating_sub(ev_tx_for_depth.capacity());
            }
        }
    }
    Ok(())
}

fn flush_keys(pending: &mut HashMap<PathBuf, Pending>, age: Duration) -> Vec<(PathBuf, Pending)> {
    let now = Instant::now();
    let keys: Vec<PathBuf> = pending
        .iter()
        .filter(|(_, op)| now.duration_since(op.at()) >= age)
        .map(|(p, _)| p.clone())
        .collect();
    keys.into_iter()
        .filter_map(|k| pending.remove(&k).map(|op| (k, op)))
        .collect()
}

async fn flush_due(
    service: &FileFlowService,
    pending: &mut HashMap<PathBuf, Pending>,
    age: Duration,
) {
    let mut due = flush_keys(pending, age);
    pair_delete_create(&mut due);
    for (path, op) in due {
        apply(service, path, op).await;
    }
}

fn ingest(
    event: Event,
    pending: &mut HashMap<PathBuf, Pending>,
    rename_from: &mut Option<(PathBuf, Instant)>,
) {
    let now = Instant::now();
    match event.kind {
        EventKind::Create(_) => {
            for p in event.paths {
                pending.insert(p, Pending::Index { at: now });
            }
        }
        EventKind::Modify(ModifyKind::Data(_)) | EventKind::Modify(ModifyKind::Any) => {
            for p in event.paths {
                pending.insert(p, Pending::Index { at: now });
            }
        }
        EventKind::Modify(ModifyKind::Name(mode)) => match mode {
            RenameMode::From => {
                if let Some(p) = event.paths.first() {
                    *rename_from = Some((p.clone(), now));
                }
            }
            RenameMode::To => {
                if let Some(to) = event.paths.first() {
                    if let Some((from, _)) = rename_from.take() {
                        pending.remove(&from);
                        pending.insert(to.clone(), Pending::Rename { from, at: now });
                    } else {
                        pending.insert(to.clone(), Pending::Index { at: now });
                    }
                }
            }
            RenameMode::Both | RenameMode::Any | RenameMode::Other => {
                if event.paths.len() >= 2 {
                    let from = event.paths[0].clone();
                    let to = event.paths[1].clone();
                    pending.remove(&from);
                    pending.insert(to.clone(), Pending::Rename { from, at: now });
                } else if let Some(p) = event.paths.first() {
                    pending.insert(p.clone(), Pending::Index { at: now });
                }
            }
        },
        EventKind::Remove(_) => {
            for p in event.paths {
                if rename_from.as_ref().is_some_and(|(from, _)| from == &p) {
                    continue;
                }
                pending.insert(p, Pending::Tombstone { at: now });
            }
        }
        _ => {}
    }
}

fn expire_rename_from(
    rename_from: &mut Option<(PathBuf, Instant)>,
    pending: &mut HashMap<PathBuf, Pending>,
) {
    if let Some((path, at)) = rename_from.as_ref() {
        if at.elapsed() > RENAME_FROM_TTL {
            let path = path.clone();
            *rename_from = None;
            pending
                .entry(path)
                .or_insert(Pending::Tombstone { at: Instant::now() });
        }
    }
}

/// Windows often emits remove+create instead of a native rename.
fn pair_delete_create(due: &mut Vec<(PathBuf, Pending)>) {
    let tombstones: Vec<PathBuf> = due
        .iter()
        .filter_map(|(p, op)| matches!(op, Pending::Tombstone { .. }).then_some(p.clone()))
        .collect();
    let indexes: Vec<PathBuf> = due
        .iter()
        .filter_map(|(p, op)| matches!(op, Pending::Index { .. }).then_some(p.clone()))
        .collect();
    if tombstones.len() != 1 || indexes.len() != 1 {
        return;
    }
    let from = &tombstones[0];
    let to = &indexes[0];
    if from == to {
        return;
    }
    if from.parent() != to.parent() {
        return;
    }
    due.retain(|(p, _)| p != from && p != to);
    due.push((
        to.clone(),
        Pending::Rename {
            from: from.clone(),
            at: Instant::now(),
        },
    ));
}

async fn apply(service: &FileFlowService, path: PathBuf, op: Pending) {
    match op {
        Pending::Index { .. } => {
            if path.is_dir() {
                for file in collect_files(&path) {
                    let _ = index_one_file(service, &file).await;
                }
            } else if path.is_file() {
                if let Err(err) = index_one_file(service, &path).await {
                    tracing::debug!(path = %path.display(), error = %err, "watch index skipped");
                }
            }
        }
        Pending::Tombstone { .. } => {
            let _ = service
                .tombstone_path(path.to_string_lossy().as_ref())
                .await;
        }
        Pending::Rename { from, .. } => {
            if let Ok(Some(id)) = service
                .resolve_path_any(from.to_string_lossy().as_ref())
                .await
            {
                let _ = service
                    .update_path(id, path.to_string_lossy().as_ref())
                    .await;
            }
            if path.is_file() {
                let _ = index_one_file(service, &path).await;
            }
        }
    }
}

async fn rescan_roots(service: &FileFlowService, roots: &[String]) {
    for root in roots {
        let p = PathBuf::from(root);
        if !p.is_dir() {
            continue;
        }
        for file in collect_files(&p) {
            let _ = index_one_file(service, &file).await;
        }
    }
}
