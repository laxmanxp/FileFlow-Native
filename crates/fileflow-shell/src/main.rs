//! Desktop shell. Talks only to FileFlowService via the client stub.

use std::num::NonZeroU32;
use std::sync::Arc;

use glutin::config::ConfigTemplateBuilder;
use glutin::context::{ContextApi, ContextAttributesBuilder, NotCurrentGlContext};
use glutin::display::GetGlDisplay;
use glutin::prelude::*;
use glutin::surface::SwapInterval;
use glutin_winit::{DisplayBuilder, GlWindow};
use winit::event::{Event, WindowEvent};
use winit::event_loop::{ControlFlow, EventLoop};
use winit::window::WindowBuilder;

use fileflow_client::{ClientError, FileFlowClient};
use fileflow_core::{DuplicateScan, LogicalFileId, LogicalFileView, RevisionInfo, SearchHit};
use fileflow_rpc::IndexReport;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[cfg(unix)]
type Stream = tokio::net::UnixStream;
#[cfg(windows)]
type Stream = tokio::net::windows::named_pipe::NamedPipeClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_loop = EventLoop::new()?;
    event_loop.set_control_flow(ControlFlow::Wait);

    let window_builder = WindowBuilder::new()
        .with_title("FileFlow")
        .with_inner_size(winit::dpi::LogicalSize::new(1100.0, 720.0));

    let template = ConfigTemplateBuilder::new();
    let display_builder = DisplayBuilder::new().with_window_builder(Some(window_builder));
    let (window, gl_config) = display_builder.build(&event_loop, template, |configs| {
        configs
            .reduce(|accum, config| {
                if config.num_samples() > accum.num_samples() {
                    config
                } else {
                    accum
                }
            })
            .unwrap()
    })?;
    let window = window.expect("gl window");

    let raw = {
        use raw_window_handle::HasRawWindowHandle;
        Some(window.raw_window_handle())
    };
    let context_attributes = ContextAttributesBuilder::new()
        .with_context_api(ContextApi::OpenGl(None))
        .build(raw);
    let not_current = unsafe {
        gl_config
            .display()
            .create_context(&gl_config, &context_attributes)?
    };
    let attrs = window.build_surface_attributes(Default::default());
    let gl_surface = unsafe {
        gl_config
            .display()
            .create_window_surface(&gl_config, &attrs)?
    };
    let gl_context = not_current.make_current(&gl_surface)?;
    let _ =
        gl_surface.set_swap_interval(&gl_context, SwapInterval::Wait(NonZeroU32::new(1).unwrap()));

    let gl = unsafe {
        glow::Context::from_loader_function_cstr(|s| gl_config.display().get_proc_address(s))
    };
    let gl = Arc::new(gl);
    let mut egui_glow = egui_glow::EguiGlow::new(&event_loop, gl, None, None);
    egui_glow.egui_ctx.set_visuals(egui::Visuals::light());

    let mut app = FileFlowApp::new();

    event_loop.run(move |event, elwt| {
        let mut redraw = false;
        match event {
            Event::WindowEvent { event, .. } => {
                let response = egui_glow.on_window_event(&window, &event);
                if response.repaint {
                    redraw = true;
                }
                match event {
                    WindowEvent::CloseRequested => elwt.exit(),
                    WindowEvent::Resized(size) if size.width > 0 && size.height > 0 => {
                        gl_surface.resize(
                            &gl_context,
                            NonZeroU32::new(size.width).unwrap(),
                            NonZeroU32::new(size.height).unwrap(),
                        );
                        redraw = true;
                    }
                    WindowEvent::RedrawRequested => redraw = true,
                    _ => {}
                }
            }
            Event::AboutToWait => {
                window.request_redraw();
            }
            _ => {}
        }

        if redraw {
            egui_glow.run(&window, |ctx| {
                app.ui(ctx);
            });
            egui_glow.paint(&window);
            let _ = gl_surface.swap_buffers(&gl_context);
        }
    })?;
    Ok(())
}

struct FileFlowApp {
    rt: Runtime,
    client: Option<Arc<Mutex<FileFlowClient<Stream>>>>,
    folder: String,
    query: String,
    status: String,
    hits: Vec<SearchHit>,
    selected: Option<LogicalFileId>,
    detail: Option<LogicalFileView>,
    tag_input: String,
    note_draft: String,
    todo_input: String,
    revisions: Vec<RevisionInfo>,
    tab_duplicates: bool,
    dup_exclude: bool,
    dup_min_size: String,
    dup_scan: Option<DuplicateScan>,
    dup_selected: Option<usize>,
    dup_pending: Option<(String, LogicalFileId, Vec<LogicalFileId>)>,
    dup_permanent: bool,
}

impl FileFlowApp {
    fn new() -> Self {
        let mut app = Self {
            rt: Runtime::new().expect("tokio runtime"),
            client: None,
            folder: String::new(),
            query: String::new(),
            status: "Connecting to FileFlowService…".into(),
            hits: Vec::new(),
            selected: None,
            detail: None,
            tag_input: String::new(),
            note_draft: String::new(),
            todo_input: String::new(),
            revisions: Vec::new(),
            tab_duplicates: false,
            dup_exclude: true,
            dup_min_size: String::new(),
            dup_scan: None,
            dup_selected: None,
            dup_pending: None,
            dup_permanent: false,
        };
        app.reconnect();
        app
    }

    fn reconnect(&mut self) {
        let cfg = fileflow_core::Config::from_env();
        match self.rt.block_on(async {
            let conn = fileflow_rpc::connect(&cfg.socket).await?;
            let mut client = FileFlowClient::new(conn);
            let health = client.health().await?;
            let watch = client.watcher_status().await.ok();
            Ok::<_, ClientError>((client, health, watch))
        }) {
            Ok((client, (status, version), watch)) => {
                self.client = Some(Arc::new(Mutex::new(client)));
                let extra = watch
                    .map(|w| {
                        format!(
                            "  watch {} root(s) paused={} q={}",
                            w.roots.len(),
                            w.paused,
                            w.queue_depth
                        )
                    })
                    .unwrap_or_default();
                self.status = format!(
                    "Connected ({status} v{version})  socket={}{extra}",
                    cfg.socket.display()
                );
            }
            Err(err) => {
                self.client = None;
                self.status = format!(
                    "Cannot reach FileFlowService ({err}). Start `fileflow-service` first."
                );
            }
        }
    }

    fn run_search(&mut self) {
        let Some(client) = self.client.clone() else {
            self.status = "Not connected. Use Reconnect after starting the service.".into();
            return;
        };
        let q = self.query.clone();
        match self.rt.block_on(async {
            let mut c = client.lock().await;
            c.search(&q).await
        }) {
            Ok(hits) => {
                self.status = format!("{} result(s)", hits.len());
                self.hits = hits;
            }
            Err(err) => self.status = format!("Error: {err}"),
        }
    }

    fn select_file(&mut self, id: LogicalFileId) {
        self.selected = Some(id);
        let Some(client) = self.client.clone() else {
            return;
        };
        match self.rt.block_on(async {
            let mut c = client.lock().await;
            let file = c.get_file(id).await?;
            let revs = if file.is_some() {
                c.list_revisions(id).await.unwrap_or_default()
            } else {
                Vec::new()
            };
            Ok::<_, ClientError>((file, revs))
        }) {
            Ok((Some(view), revs)) => {
                self.note_draft = view.note.clone();
                self.detail = Some(view);
                self.revisions = revs;
            }
            Ok((None, _)) => {
                self.status = "File not found".into();
                self.revisions.clear();
            }
            Err(err) => self.status = format!("Error: {err}"),
        }
    }

    fn index_folder(&mut self) {
        let Some(client) = self.client.clone() else {
            self.status = "Not connected. Use Reconnect after starting the service.".into();
            return;
        };
        let folder = self.folder.clone();
        match self.rt.block_on(async {
            let mut c = client.lock().await;
            if std::path::Path::new(&folder).is_dir() {
                c.index_folder(&folder).await
            } else {
                c.index_path(&folder).await
            }
        }) {
            Ok(IndexReport {
                indexed, skipped, ..
            }) => {
                let extra = self
                    .client
                    .clone()
                    .and_then(|c| {
                        self.rt
                            .block_on(async { c.lock().await.watcher_status().await.ok() })
                    })
                    .map(|w| format!("; watching {} root(s)", w.roots.len()))
                    .unwrap_or_default();
                self.status = format!("Indexed {indexed} file(s), skipped {skipped}{extra}");
            }
            Err(err) => self.status = format!("Error: {err}"),
        }
    }

    fn set_watch_paused(&mut self, pause: bool) {
        let Some(client) = self.client.clone() else {
            return;
        };
        let result = self.rt.block_on(async {
            let mut c = client.lock().await;
            if pause {
                c.pause_watcher().await?;
            } else {
                c.resume_watcher().await?;
            }
            c.watcher_status().await
        });
        match result {
            Ok(st) => {
                self.status = format!(
                    "Watcher paused={} roots={} q={} err={:?}",
                    st.paused,
                    st.roots.len(),
                    st.queue_depth,
                    st.last_error
                );
            }
            Err(err) => self.status = format!("Watcher error: {err}"),
        }
    }

    fn ui(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.heading("FileFlow");
                ui.label("Add → Find → Open → Edit metadata → Duplicates");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Reconnect").clicked() {
                        self.reconnect();
                    }
                });
            });
            ui.horizontal(|ui| {
                ui.label("Folder");
                ui.add(egui::TextEdit::singleline(&mut self.folder).desired_width(420.0));
                if ui.button("Browse…").clicked() {
                    if let Some(path) = pick_folder() {
                        self.folder = path;
                    }
                }
                if ui.button("Index").clicked() {
                    self.index_folder();
                }
                if ui.button("Pause watch").clicked() {
                    self.set_watch_paused(true);
                }
                if ui.button("Resume watch").clicked() {
                    self.set_watch_paused(false);
                }
            });
            ui.horizontal(|ui| {
                ui.label("Search");
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.query)
                        .desired_width(420.0)
                        .hint_text("plain  tag:work  todo:x  notes:y  ext:pdf"),
                );
                if ui.button("Find").clicked()
                    || (resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                {
                    self.run_search();
                }
            });
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab_duplicates, false, "Find");
                ui.selectable_value(&mut self.tab_duplicates, true, "Duplicates");
            });
            ui.label(&self.status);
            ui.add_space(4.0);
        });

        let mut clicked = None;
        let mut remove_tag = None;
        let mut add_tag = false;
        let mut save_note = false;
        let mut add_todo = false;
        let mut toggle_todo = None;
        let mut open_path: Option<String> = None;
        let mut reveal_path: Option<String> = None;
        let mut vault_toggle: Option<bool> = None;
        let mut make_current: Option<i64> = None;
        let mut export_rev: Option<i64> = None;
        let mut verify_rev: Option<i64> = None;

        if self.tab_duplicates {
            self.ui_duplicates(ctx);
            return;
        }

        egui::SidePanel::left("results")
            .resizable(true)
            .default_width(380.0)
            .show(ctx, |ui| {
                ui.heading("Results");
                egui::ScrollArea::vertical().show(ui, |ui| {
                    for hit in &self.hits {
                        let selected = self.selected == Some(hit.logical_file_id);
                        let extra = if hit.tags.is_empty() {
                            String::new()
                        } else {
                            format!("  [{}]", hit.tags.join(", "))
                        };
                        if ui
                            .selectable_label(selected, format!("{}{extra}", hit.path))
                            .clicked()
                        {
                            clicked = Some(hit.logical_file_id);
                        }
                    }
                });
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("File");
            let Some(view) = self.detail.clone() else {
                ui.label("Select a search result.");
                return;
            };
            ui.monospace(format!("logical_file_id = {}", view.logical_file_id));
            for p in &view.paths {
                ui.label(format!("path: {p}"));
            }
            if let Some(sha) = &view.sha256 {
                ui.monospace(format!("sha256: {sha}"));
            }
            if let Some(size) = view.size {
                ui.label(format!("size: {size} bytes"));
            }

            ui.horizontal(|ui| {
                if ui.button("Open").clicked() {
                    open_path = view.paths.first().cloned();
                }
                if ui.button("Reveal").clicked() {
                    reveal_path = view.paths.first().cloned();
                }
            });

            ui.separator();
            ui.label("Tags");
            ui.horizontal_wrapped(|ui| {
                for tag in &view.tags {
                    ui.label(format!("#{tag}"));
                    if ui.small_button("x").clicked() {
                        remove_tag = Some(tag.clone());
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.add(egui::TextEdit::singleline(&mut self.tag_input).hint_text("new tag"));
                if ui.button("Add tag").clicked() {
                    add_tag = true;
                }
            });

            ui.separator();
            ui.label("Notes");
            ui.add(
                egui::TextEdit::multiline(&mut self.note_draft)
                    .desired_rows(5)
                    .desired_width(f32::INFINITY),
            );
            if ui.button("Save note").clicked() {
                save_note = true;
            }

            ui.separator();
            ui.label("Todos");
            for todo in &view.todos {
                ui.horizontal(|ui| {
                    let mut done = todo.done;
                    if ui.checkbox(&mut done, &todo.title).changed() {
                        toggle_todo = Some((todo.id, done));
                    }
                });
            }
            ui.horizontal(|ui| {
                ui.add(egui::TextEdit::singleline(&mut self.todo_input).hint_text("new todo"));
                if ui.button("Add todo").clicked() {
                    add_todo = true;
                }
            });

            ui.separator();
            ui.label("Vault / versions");
            let mut vaulted = view.vaulted;
            if ui
                .checkbox(&mut vaulted, "Vault this file (keep immutable revisions)")
                .changed()
            {
                vault_toggle = Some(vaulted);
            }
            ui.label("Select which revision should become current. Other versions are kept.");
            egui::ScrollArea::vertical()
                .max_height(160.0)
                .show(ui, |ui| {
                    for rev in &self.revisions {
                        let mark = if rev.is_current { " (current)" } else { "" };
                        ui.horizontal(|ui| {
                            ui.monospace(format!(
                                "#{}  {}  {}  {} bytes{mark}",
                                rev.revision_id,
                                format_unix_utc(rev.created_at),
                                &rev.sha256[..rev.sha256.len().min(12)],
                                rev.size
                            ));
                            if ui.small_button("Make current").clicked() {
                                make_current = Some(rev.revision_id);
                            }
                            if ui.small_button("Export…").clicked() {
                                export_rev = Some(rev.revision_id);
                            }
                            if ui.small_button("Verify").clicked() {
                                verify_rev = Some(rev.revision_id);
                            }
                        });
                    }
                });
            if self.revisions.is_empty() {
                ui.label("No revisions listed. Vault the file to store content objects.");
            }
        });

        if let Some(id) = clicked {
            self.select_file(id);
        }
        if let Some(id) = self.selected {
            if let Some(client) = self.client.clone() {
                if let Some(tag) = remove_tag {
                    let _ = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.remove_tag(id, &tag).await
                    });
                    self.select_file(id);
                }
                if add_tag {
                    let tag = self.tag_input.clone();
                    let _ = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.add_tag(id, &tag).await
                    });
                    self.tag_input.clear();
                    self.select_file(id);
                }
                if save_note {
                    let body = self.note_draft.clone();
                    let _ = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.set_note(id, &body).await
                    });
                    self.select_file(id);
                }
                if add_todo {
                    let title = self.todo_input.clone();
                    let _ = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.add_todo(id, &title).await
                    });
                    self.todo_input.clear();
                    self.select_file(id);
                }
                if let Some((todo_id, done)) = toggle_todo {
                    let _ = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.set_todo_done(todo_id, done).await
                    });
                    self.select_file(id);
                }
                if let Some(on) = vault_toggle {
                    let result = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        if on {
                            c.add_to_vault(id).await
                        } else {
                            c.remove_from_vault(id).await
                        }
                    });
                    match result {
                        Ok(()) => {
                            self.status = if on {
                                "File vaulted; current content stored as an immutable object."
                                    .into()
                            } else {
                                "Vault membership removed; historical objects are kept until prune."
                                    .into()
                            };
                        }
                        Err(err) => self.status = format!("Vault failed: {err}"),
                    }
                    self.select_file(id);
                }
                if let Some(rev) = make_current {
                    let result = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.set_current_revision(id, rev).await
                    });
                    match result {
                        Ok(()) => {
                            self.status = format!(
                                "Revision {rev} is now current (bytes restored to the current path when possible)."
                            );
                        }
                        Err(err) => self.status = format!("Make current failed: {err}"),
                    }
                    self.select_file(id);
                }
                if let Some(rev) = export_rev {
                    let dest = self.detail.as_ref().and_then(|v| v.paths.first()).map(|p| {
                        let path = std::path::Path::new(p);
                        let stem = path.file_stem().unwrap_or_default().to_string_lossy();
                        let ext = path
                            .extension()
                            .map(|e| format!(".{}", e.to_string_lossy()))
                            .unwrap_or_default();
                        path.parent()
                            .unwrap_or_else(|| std::path::Path::new("."))
                            .join(format!("{stem}.r{rev}{ext}"))
                            .display()
                            .to_string()
                    });
                    if let Some(dest) = dest {
                        let result = self.rt.block_on(async {
                            let mut c = client.lock().await;
                            c.export_revision(id, rev, &dest).await
                        });
                        match result {
                            Ok(()) => self.status = format!("Exported revision {rev} to {dest}"),
                            Err(err) => self.status = format!("Export failed: {err}"),
                        }
                    } else {
                        self.status = "Export needs a current path.".into();
                    }
                }
                if let Some(rev) = verify_rev {
                    let result = self.rt.block_on(async {
                        let mut c = client.lock().await;
                        c.verify_revision(id, rev).await
                    });
                    match result {
                        Ok(report) => {
                            self.status = format!(
                                "Verify revision {rev}: {} ({})",
                                if report.ok { "ok" } else { "FAILED" },
                                report.message
                            );
                        }
                        Err(err) => self.status = format!("Verify failed: {err}"),
                    }
                }
            }
        }
        if let Some(path) = open_path {
            if let Err(err) = open_path_os(&path) {
                self.status = format!("Open failed: {err}");
            }
        }
        if let Some(path) = reveal_path {
            reveal(&path);
        }
    }

    fn run_duplicate_scan(&mut self) {
        let Some(client) = self.client.clone() else {
            self.status = "Not connected. Use Reconnect after starting the service.".into();
            return;
        };
        let min_size = self.dup_min_size.trim().parse::<u64>().ok();
        let exclude = self.dup_exclude;
        match self.rt.block_on(async {
            let mut c = client.lock().await;
            c.find_duplicates(None, None, None, Some(exclude), min_size, Some(false))
                .await
        }) {
            Ok(scan) => {
                self.status = format!(
                    "{} identical-content group(s); {} reclaimable (user-confirmed delete only; no Clean all)",
                    scan.group_count,
                    format_bytes(scan.reclaimable_bytes)
                );
                self.dup_scan = Some(scan);
                self.dup_selected = None;
                self.dup_pending = None;
            }
            Err(err) => self.status = format!("Duplicate scan failed: {err}"),
        }
    }

    fn ui_duplicates(&mut self, ctx: &egui::Context) {
        let mut scan_clicked = false;
        let mut keep_choice: Option<(String, LogicalFileId, Vec<LogicalFileId>)> = None;
        let mut confirm_go = false;
        let mut cancel = false;
        let mut select_group: Option<usize> = None;

        egui::SidePanel::left("dup_groups")
            .resizable(true)
            .default_width(420.0)
            .show(ctx, |ui| {
                ui.heading("Duplicate groups");
                ui.label("Same SHA-256 → same content. You choose what to retain.");
                ui.checkbox(
                    &mut self.dup_exclude,
                    "Exclude common build/VCS dirs (.git, node_modules, vendor, target, build, dist, .cache)",
                );
                ui.horizontal(|ui| {
                    ui.label("Min size (bytes)");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.dup_min_size)
                            .desired_width(100.0)
                            .hint_text("0"),
                    );
                    if ui.button("Scan catalog").clicked() {
                        scan_clicked = true;
                    }
                });
                if let Some(scan) = &self.dup_scan {
                    ui.label(format!(
                        "{} group(s) · {} members · {} reclaimable",
                        scan.group_count,
                        scan.member_count,
                        format_bytes(scan.reclaimable_bytes)
                    ));
                    if !scan.exclude_patterns_applied.is_empty() {
                        ui.label(format!(
                            "Path-segment exclusions: {}",
                            scan.exclude_patterns_applied.join(", ")
                        ));
                    }
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for (i, g) in scan.groups.iter().enumerate() {
                            let n = g.members.len();
                            let selected = self.dup_selected == Some(i);
                            if ui
                                .selectable_label(
                                    selected,
                                    format!(
                                        "{n} identical files · {} · {}",
                                        format_bytes(g.size),
                                        format_bytes(g.reclaimable_bytes)
                                    ),
                                )
                                .clicked()
                            {
                                select_group = Some(i);
                            }
                        }
                    });
                } else {
                    ui.label("Scan uses hashes already in the catalog (index a folder first).");
                }
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Group members");
            ui.label("Deletes are per-group and confirmed. There is no Clean all.");
            ui.checkbox(
                &mut self.dup_permanent,
                "Permanent delete if trash/recycle is unavailable",
            );
            let Some(scan) = &self.dup_scan else {
                ui.label("Run a scan to list groups.");
                return;
            };
            let Some(idx) = self.dup_selected else {
                ui.label("Select a group.");
                return;
            };
            let Some(group) = scan.groups.get(idx) else {
                return;
            };
            ui.monospace(format!("sha256: {}", group.sha256));
            ui.label(format!(
                "{} identical files found · size {} · reclaimable {}",
                group.members.len(),
                format_bytes(group.size),
                format_bytes(group.reclaimable_bytes)
            ));
            for m in &group.members {
                ui.horizontal(|ui| {
                    ui.label(&m.path);
                    if ui.small_button("Keep this").clicked() {
                        let delete: Vec<_> = group
                            .members
                            .iter()
                            .map(|x| x.logical_file_id)
                            .filter(|id| *id != m.logical_file_id)
                            .collect();
                        keep_choice = Some((group.sha256.clone(), m.logical_file_id, delete));
                    }
                });
            }
            if let Some((sha, keep, delete)) = &self.dup_pending {
                ui.separator();
                ui.colored_label(
                    egui::Color32::from_rgb(120, 40, 40),
                    format!(
                        "Confirm: trash/delete {} other copy(ies) of {sha:.12}…? Kept logical_file_id={keep}. Other versions are not auto-removed from the vault.",
                        delete.len()
                    ),
                );
                ui.horizontal(|ui| {
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                    if ui.button("Delete the other copies").clicked() {
                        confirm_go = true;
                    }
                });
            }
        });

        if scan_clicked {
            self.run_duplicate_scan();
        }
        if let Some(i) = select_group {
            self.dup_selected = Some(i);
            self.dup_pending = None;
        }
        if let Some(p) = keep_choice {
            self.dup_pending = Some(p);
        }
        if cancel {
            self.dup_pending = None;
        }
        if confirm_go {
            if let Some((sha, keep, delete)) = self.dup_pending.take() {
                self.apply_resolve(sha, keep, delete);
            }
        }
    }

    fn apply_resolve(&mut self, sha: String, keep: LogicalFileId, delete: Vec<LogicalFileId>) {
        let Some(client) = self.client.clone() else {
            self.status = "Not connected.".into();
            return;
        };
        let permanent = self.dup_permanent;
        let result = self.rt.block_on(async {
            let mut c = client.lock().await;
            c.resolve_duplicate_group(&sha, Some(keep), delete, true, permanent, false)
                .await
        });
        match result {
            Ok(report) => {
                self.status = format!(
                    "Kept {keep}; {} copy(ies) removed (trash={} permanent={}). {}",
                    report.deleted.len(),
                    report.used_trash,
                    report.permanent,
                    report.messages.join("; ")
                );
                self.run_duplicate_scan();
            }
            Err(err) => self.status = format!("Resolve failed: {err}"),
        }
    }
}

fn format_bytes(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1024 * 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else if n < 1024 * 1024 * 1024 {
        format!("{:.1} MiB", n as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GiB", n as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn format_unix_utc(secs: i64) -> String {
    if secs < 0 {
        return secs.to_string();
    }
    let secs = secs as u64;
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}Z")
}

/// Howard Hinnant civil-from-days (days since Unix epoch).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

fn pick_folder() -> Option<String> {
    #[cfg(target_os = "windows")]
    {
        let output = std::process::Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                "Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.FolderBrowserDialog; if ($d.ShowDialog() -eq 'OK') { $d.SelectedPath }",
            ])
            .output()
            .ok()?;
        let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        for cmd in [
            ["zenity", "--file-selection", "--directory"].as_slice(),
            ["kdialog", "--getexistingdirectory", "."].as_slice(),
        ] {
            if let Ok(output) = std::process::Command::new(cmd[0]).args(&cmd[1..]).output() {
                if output.status.success() {
                    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
        }
        None
    }
}

fn open_path_os(path: &str) -> std::io::Result<()> {
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", path])
            .spawn()
            .map(|_| ())
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg(path)
            .spawn()
            .map(|_| ())
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        std::process::Command::new("xdg-open")
            .arg(path)
            .spawn()
            .map(|_| ())
    }
}

fn reveal(path: &str) {
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .spawn();
    }
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open")
            .args(["-R", path])
            .spawn();
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = open_path_os(&parent.display().to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn shell_manifest_does_not_link_rusqlite() {
        let manifest = include_str!("../Cargo.toml");
        assert!(
            !manifest.contains("rusqlite"),
            "UI must not depend on rusqlite"
        );
        assert!(
            !manifest.contains("fileflow-catalog"),
            "UI must not depend on the catalog crate"
        );
        assert!(manifest.contains("fileflow-client"));
    }

    #[test]
    fn unix_utc_formats_epoch() {
        assert_eq!(super::format_unix_utc(0), "1970-01-01 00:00:00Z");
        assert_eq!(super::format_unix_utc(86_400), "1970-01-02 00:00:00Z");
    }
}
