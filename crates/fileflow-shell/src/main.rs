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
use fileflow_core::{LogicalFileId, LogicalFileView, SearchHit};
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
            Ok::<_, ClientError>((client, health))
        }) {
            Ok((client, (status, version))) => {
                self.client = Some(Arc::new(Mutex::new(client)));
                self.status = format!(
                    "Connected ({status} v{version})  socket={}",
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
            c.get_file(id).await
        }) {
            Ok(Some(view)) => {
                self.note_draft = view.note.clone();
                self.detail = Some(view);
            }
            Ok(None) => self.status = "File not found".into(),
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
                self.status = format!("Indexed {indexed} file(s), skipped {skipped}");
            }
            Err(err) => self.status = format!("Error: {err}"),
        }
    }

    fn ui(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.heading("FileFlow");
                ui.label("Add → Find → Open → Edit metadata");
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
}
