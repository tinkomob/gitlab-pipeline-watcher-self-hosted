#[cfg(windows)]
use std::process::Command;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Instant, SystemTime},
};

use directories::ProjectDirs;
use eframe::egui;
use keyring::Entry;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc as tokio_mpsc, watch};
use tray_icon::{
    Icon, MouseButton, TrayIcon, TrayIconBuilder, TrayIconEvent,
    menu::{Menu, MenuEvent, MenuItem},
};

#[cfg(windows)]
use std::sync::atomic::AtomicIsize;

#[cfg(windows)]
use raw_window_handle::{HasWindowHandle, RawWindowHandle};

#[cfg(windows)]
static NOTIFICATION_OPEN_EVENT: AtomicIsize = AtomicIsize::new(0);

#[cfg(windows)]
const OPEN_EVENT_NAME: &str = "Local\\PipelineWatcherOpenEvent";

const KEYRING_SERVICE: &str = "pipeline-watcher";
const KEYRING_USER: &str = "gitlab-token";

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub base_url: String,
    #[serde(skip)]
    pub token: String,
    #[serde(default = "default_poll_seconds")]
    pub poll_seconds: u64,
    #[serde(default)]
    pub auto_start: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            token: String::new(),
            poll_seconds: default_poll_seconds(),
            auto_start: false,
        }
    }
}

impl Config {
    pub fn is_ready(&self) -> bool {
        !self.base_url.trim().is_empty() && !self.token.trim().is_empty()
    }
}

fn default_poll_seconds() -> u64 {
    15
}

#[derive(Clone)]
pub struct Pipeline {
    pub project_id: i64,
    pub path: String,
    pub id: i64,
    pub status: String,
    pub branch: String,
    pub url: String,
    pub updated_at: String,
}

impl Pipeline {
    pub fn is_active_status(status: &str) -> bool {
        matches!(
            status,
            "created"
                | "waiting_for_resource"
                | "preparing"
                | "pending"
                | "running"
                | "scheduled"
                | "canceling"
        )
    }
}

pub struct PollResult {
    pub pipelines: Vec<Pipeline>,
    pub status: String,
    pub error: String,
    pub updated_at: Option<SystemTime>,
}

#[derive(Clone)]
pub struct PipelineJob {
    pub stage: String,
    pub name: String,
    pub status: String,
    pub url: String,
}

pub enum WorkerCommand {
    FetchJobs { project_id: i64, pipeline_id: i64 },
}

pub enum WorkerUpdate {
    Snapshot(PollResult),
    Jobs {
        project_id: i64,
        pipeline_id: i64,
        result: Result<Vec<PipelineJob>, String>,
    },
}

impl PollResult {
    pub fn error(message: &str) -> Self {
        Self {
            pipelines: Vec::new(),
            status: "Disconnected".into(),
            error: message.into(),
            updated_at: None,
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Tab {
    Running,
    Failed,
    Success,
}

pub struct PipelineWatcher {
    config: Config,
    token_input: String,
    config_tx: watch::Sender<Config>,
    command_tx: tokio_mpsc::UnboundedSender<WorkerCommand>,
    result_rx: mpsc::Receiver<WorkerUpdate>,
    pipelines: Vec<Pipeline>,
    expanded: HashSet<(i64, i64)>,
    loading_jobs: HashSet<(i64, i64)>,
    jobs: HashMap<(i64, i64), Vec<PipelineJob>>,
    job_errors: HashMap<(i64, i64), String>,
    status: String,
    error: String,
    last_updated: Option<SystemTime>,
    tab: Tab,
    tray: Option<TrayIcon>,
    #[cfg(windows)]
    hwnd: Option<usize>,
    open_requested: Arc<AtomicBool>,
    quit_requested: Arc<AtomicBool>,
    restore_window_level_at: Option<Instant>,
    config_path: Option<PathBuf>,
    tray_count: Option<usize>,
}

impl PipelineWatcher {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let (mut config, config_path) = load_config();
        let token_input = load_token().unwrap_or_default();
        config.token = token_input.clone();
        let (config_tx, config_rx) = watch::channel(config.clone());
        let (command_tx, command_rx) = tokio_mpsc::unbounded_channel();
        let (result_tx, result_rx) = mpsc::channel();
        crate::gitlab::start(config_rx, command_rx, result_tx, cc.egui_ctx.clone());

        let open = MenuItem::new("Open Pipeline Watcher", true, None);
        let quit = MenuItem::new("Exit", true, None);
        let menu = Menu::new();
        let _ = menu.append(&open);
        let _ = menu.append(&quit);
        let tray = TrayIconBuilder::new()
            .with_menu(Box::new(menu))
            .with_menu_on_left_click(false)
            .with_tooltip("Pipeline Watcher")
            .with_icon(tray_icon(0))
            .build()
            .ok();
        let open_requested = Arc::new(AtomicBool::new(false));
        #[cfg(windows)]
        let hwnd = cc
            .window_handle()
            .ok()
            .and_then(|handle| match handle.as_raw() {
                RawWindowHandle::Win32(handle) => Some(handle.hwnd.get() as usize),
                _ => None,
            });
        #[cfg(windows)]
        if let Some(hwnd) = hwnd {
            let requested = open_requested.clone();
            let ctx = cc.egui_ctx.clone();
            let event = NOTIFICATION_OPEN_EVENT.load(Ordering::SeqCst);
            if event != 0 {
                std::thread::spawn(move || {
                    while unsafe {
                        windows_sys::Win32::System::Threading::WaitForSingleObject(
                            event as windows_sys::Win32::Foundation::HANDLE,
                            windows_sys::Win32::System::Threading::INFINITE,
                        ) == windows_sys::Win32::Foundation::WAIT_OBJECT_0
                    } {
                        raise_window(hwnd);
                        requested.store(true, Ordering::SeqCst);
                        ctx.request_repaint();
                    }
                });
            }
        }
        let quit_requested = Arc::new(AtomicBool::new(false));
        let open_menu_id = open.id().clone();
        let quit_menu_id = quit.id().clone();
        let menu_open = open_requested.clone();
        let menu_quit = quit_requested.clone();
        let menu_ctx = cc.egui_ctx.clone();
        MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
            if event.id == open_menu_id {
                #[cfg(windows)]
                if let Some(hwnd) = hwnd {
                    raise_window(hwnd);
                }
                menu_open.store(true, Ordering::SeqCst);
                menu_ctx.request_repaint();
            } else if event.id == quit_menu_id {
                menu_quit.store(true, Ordering::SeqCst);
                menu_ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                menu_ctx.request_repaint();
            }
        }));
        let tray_open = open_requested.clone();
        let tray_ctx = cc.egui_ctx.clone();
        TrayIconEvent::set_event_handler(Some(move |event: TrayIconEvent| {
            if matches!(
                event,
                TrayIconEvent::Click {
                    button: MouseButton::Left,
                    ..
                } | TrayIconEvent::DoubleClick {
                    button: MouseButton::Left,
                    ..
                }
            ) {
                #[cfg(windows)]
                if let Some(hwnd) = hwnd {
                    raise_window(hwnd);
                }
                tray_open.store(true, Ordering::SeqCst);
                tray_ctx.request_repaint();
            }
        }));

        Self {
            config,
            token_input,
            config_tx,
            command_tx,
            result_rx,
            pipelines: Vec::new(),
            expanded: HashSet::new(),
            loading_jobs: HashSet::new(),
            jobs: HashMap::new(),
            job_errors: HashMap::new(),
            status: "Enter your GitLab URL and access token".into(),
            error: String::new(),
            last_updated: None,
            tab: Tab::Running,
            tray,
            #[cfg(windows)]
            hwnd,
            open_requested,
            quit_requested,
            restore_window_level_at: None,
            config_path,
            tray_count: None,
        }
    }

    fn save_and_connect(&mut self) {
        self.config.token = self.token_input.trim().to_owned();
        self.config.base_url = crate::gitlab::without_local_host_label(
            self.config.base_url.trim().trim_end_matches('/'),
        );
        if let Err(error) = self.persist_config() {
            self.error = error;
        }
        if self.config.token.is_empty() {
            self.error = "Enter a GitLab token with read_api access".into();
            return;
        }
        match Entry::new(KEYRING_SERVICE, KEYRING_USER)
            .and_then(|entry| entry.set_password(&self.config.token))
        {
            Ok(()) => {
                self.error.clear();
                let _ = self.config_tx.send(self.config.clone());
                self.status = "Connecting…".into();
            }
            Err(error) => {
                self.error =
                    format!("Could not save the token to the system credential store: {error}")
            }
        }
    }

    fn persist_config(&self) -> Result<(), String> {
        let Some(path) = &self.config_path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("Could not create the settings directory: {error}"))?;
        }
        let bytes = serde_json::to_vec_pretty(&self.config)
            .map_err(|error| format!("Could not serialize settings: {error}"))?;
        fs::write(path, bytes).map_err(|error| format!("Could not save settings: {error}"))
    }

    fn handle_tray_events(&mut self, ctx: &egui::Context) {
        if self.open_requested.swap(false, Ordering::SeqCst) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd(egui::ViewportCommand::WindowLevel(
                egui::WindowLevel::AlwaysOnTop,
            ));
            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            self.restore_window_level_at =
                Some(Instant::now() + std::time::Duration::from_millis(1_500));
        }
        if self
            .restore_window_level_at
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            ctx.send_viewport_cmd(egui::ViewportCommand::WindowLevel(
                egui::WindowLevel::Normal,
            ));
            self.restore_window_level_at = None;
        }
    }

    fn apply_results(&mut self) {
        while let Ok(result) = self.result_rx.try_recv() {
            match result {
                WorkerUpdate::Snapshot(result) => {
                    self.status = result.status;
                    self.error = result.error;
                    if let Some(updated_at) = result.updated_at {
                        self.pipelines = result.pipelines;
                        self.last_updated = Some(updated_at);
                        let active: Vec<_> = self
                            .pipelines
                            .iter()
                            .filter(|pipeline| Pipeline::is_active_status(&pipeline.status))
                            .map(|pipeline| (pipeline.project_id, pipeline.id))
                            .collect();
                        for (project_id, pipeline_id) in active {
                            self.request_jobs(project_id, pipeline_id);
                        }
                    }
                }
                WorkerUpdate::Jobs {
                    project_id,
                    pipeline_id,
                    result,
                } => {
                    let key = (project_id, pipeline_id);
                    self.loading_jobs.remove(&key);
                    match result {
                        Ok(jobs) => {
                            self.job_errors.remove(&key);
                            self.jobs.insert(key, jobs);
                        }
                        Err(error) => {
                            self.job_errors.insert(key, error);
                        }
                    }
                }
            }
        }
    }

    fn request_jobs(&mut self, project_id: i64, pipeline_id: i64) {
        let key = (project_id, pipeline_id);
        if !self.loading_jobs.insert(key) {
            return;
        }
        self.job_errors.remove(&key);
        if self
            .command_tx
            .send(WorkerCommand::FetchJobs {
                project_id,
                pipeline_id,
            })
            .is_err()
        {
            self.loading_jobs.remove(&key);
            self.job_errors
                .insert(key, "Could not start loading pipeline stages".into());
        }
    }

    fn update_tray(&mut self, _ctx: &egui::Context) {
        let active = self
            .pipelines
            .iter()
            .filter(|pipeline| Pipeline::is_active_status(&pipeline.status))
            .count();
        if self.tray_count != Some(active) {
            self.tray_count = Some(active);
            #[cfg(windows)]
            if let Some(hwnd) = self.hwnd {
                set_taskbar_overlay(hwnd, active);
            }
            if let Some(tray) = &self.tray {
                let _ = tray.set_icon(Some(tray_icon(active)));
                let _ = tray.set_tooltip(Some(&format!("Pipeline Watcher: {active} running")));
            }
        }
    }
}

impl eframe::App for PipelineWatcher {
    fn ui(&mut self, root_ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = root_ui.ctx().clone();
        self.handle_tray_events(&ctx);
        self.apply_results();

        if ctx.input(|input| input.viewport().close_requested())
            && !self.quit_requested.load(Ordering::SeqCst)
        {
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
        }

        egui::CentralPanel::default().show(root_ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Pipeline Watcher");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!(
                        "{} running",
                        self.pipelines
                            .iter()
                            .filter(|p| Pipeline::is_active_status(&p.status))
                            .count()
                    ));
                });
            });
            ui.separator();

            egui::CollapsingHeader::new("GitLab Connection")
                .default_open(!self.config.is_ready())
                .show(ui, |ui| {
                    egui::Grid::new("settings")
                        .num_columns(2)
                        .spacing([12.0, 8.0])
                        .show(ui, |ui| {
                            ui.label("GitLab URL");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.config.base_url)
                                    .hint_text("https://gitlab.example.com"),
                            );
                            ui.end_row();
                            ui.label("API Token");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.token_input)
                                    .password(true)
                                    .hint_text("glpat-…"),
                            );
                            ui.end_row();
                            ui.label("Refresh interval (seconds)");
                            ui.add(egui::Slider::new(&mut self.config.poll_seconds, 10..=300));
                            ui.end_row();
                        });
                    #[cfg(windows)]
                    {
                        let previous = self.config.auto_start;
                        if ui
                            .checkbox(&mut self.config.auto_start, "Start with Windows")
                            .changed()
                        {
                            match set_auto_start(self.config.auto_start) {
                                Ok(()) => {
                                    if let Err(error) = self.persist_config() {
                                        let _ = set_auto_start(previous);
                                        self.config.auto_start = previous;
                                        self.error = error;
                                    }
                                }
                                Err(error) => {
                                    self.config.auto_start = previous;
                                    self.error = error;
                                }
                            }
                        }
                    }
                    if ui.button("Save and connect").clicked() {
                        self.save_and_connect();
                    }
                });

            ui.horizontal(|ui| {
                ui.label(&self.status);
                if let Some(updated) = self.last_updated.and_then(|time| time.elapsed().ok()) {
                    ui.label(format!("Updated {}s ago", updated.as_secs()));
                }
            });
            if !self.error.is_empty() {
                ui.colored_label(egui::Color32::LIGHT_RED, &self.error);
            }

            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, Tab::Running, "Running");
                ui.selectable_value(&mut self.tab, Tab::Failed, "Failed");
                ui.selectable_value(&mut self.tab, Tab::Success, "Successful");
            });
            ui.separator();

            let mut visible: Vec<Pipeline> = self
                .pipelines
                .iter()
                .filter(|pipeline| match self.tab {
                    Tab::Running => Pipeline::is_active_status(&pipeline.status),
                    Tab::Failed => pipeline.status == "failed",
                    Tab::Success => pipeline.status == "success",
                })
                .cloned()
                .collect();
            visible.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
            egui::ScrollArea::vertical().show(ui, |ui| {
                if visible.is_empty() {
                    ui.centered_and_justified(|ui| ui.label("No pipelines in this category"));
                }
                for pipeline in &visible {
                    let key = (pipeline.project_id, pipeline.id);
                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.strong(format!("{} · #{}", pipeline.path, pipeline.id));
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    ui.label(status_label(&pipeline.status));
                                },
                            );
                        });
                        ui.horizontal(|ui| {
                            ui.label(format!(
                                "Branch: {}",
                                if pipeline.branch.is_empty() {
                                    "—"
                                } else {
                                    &pipeline.branch
                                }
                            ));
                            if !pipeline.url.is_empty() && ui.link("Open in GitLab").clicked() {
                                ui.ctx().open_url(egui::OpenUrl::new_tab(&pipeline.url));
                            }
                        });

                        let is_active = Pipeline::is_active_status(&pipeline.status);
                        let is_expanded = self.expanded.contains(&key);
                        if !is_active {
                            if ui
                                .small_button(if is_expanded {
                                    "Hide stages"
                                } else {
                                    "Show stages"
                                })
                                .clicked()
                            {
                                if is_expanded {
                                    self.expanded.remove(&key);
                                } else {
                                    self.expanded.insert(key);
                                    if !self.jobs.contains_key(&key) {
                                        self.request_jobs(key.0, key.1);
                                    }
                                }
                            }
                        }

                        if is_active || self.expanded.contains(&key) {
                            if let Some(jobs) = self.jobs.get(&key) {
                                if let Some(error) = self.job_errors.get(&key) {
                                    ui.colored_label(egui::Color32::LIGHT_RED, error);
                                }
                                if jobs.is_empty() {
                                    ui.label("This pipeline has no jobs.");
                                } else {
                                    let mut stages: Vec<(&str, Vec<&PipelineJob>)> = Vec::new();
                                    for job in jobs {
                                        if let Some((_, stage_jobs)) =
                                            stages.iter_mut().find(|(stage, _)| *stage == job.stage)
                                        {
                                            stage_jobs.push(job);
                                        } else {
                                            stages.push((&job.stage, vec![job]));
                                        }
                                    }
                                    for (stage, stage_jobs) in stages {
                                        let stage_color = if stage_jobs
                                            .iter()
                                            .any(|job| job.status == "failed")
                                        {
                                            Some(egui::Color32::LIGHT_RED)
                                        } else if stage_jobs
                                            .iter()
                                            .all(|job| job.status == "success")
                                        {
                                            Some(egui::Color32::LIGHT_GREEN)
                                        } else {
                                            None
                                        };
                                        if let Some(color) = stage_color {
                                            ui.colored_label(
                                                color,
                                                egui::RichText::new(stage).strong(),
                                            );
                                        } else {
                                            ui.strong(stage);
                                        }
                                        for job in stage_jobs {
                                            ui.horizontal(|ui| {
                                                ui.label(format!("  {} —", job.name));
                                                let text = status_label(&job.status);
                                                if let Some(color) = job_status_color(&job.status) {
                                                    ui.colored_label(color, text);
                                                } else {
                                                    ui.label(text);
                                                }
                                                if !job.url.is_empty()
                                                    && ui.link("Open job").clicked()
                                                {
                                                    ui.ctx()
                                                        .open_url(egui::OpenUrl::new_tab(&job.url));
                                                }
                                            });
                                        }
                                    }
                                }
                            } else if !self.loading_jobs.contains(&key)
                                && let Some(error) = self.job_errors.get(&key).cloned()
                            {
                                ui.horizontal(|ui| {
                                    ui.colored_label(egui::Color32::LIGHT_RED, error);
                                    if ui.small_button("Retry").clicked() {
                                        self.request_jobs(key.0, key.1);
                                    }
                                });
                            }
                        }
                    });
                    ui.add_space(4.0);
                }
            });
        });

        if !self.loading_jobs.is_empty() {
            egui::Area::new(egui::Id::new("jobs_loading_indicator"))
                .order(egui::Order::Foreground)
                .anchor(egui::Align2::RIGHT_BOTTOM, egui::vec2(-12.0, -12.0))
                .show(&ctx, |ui| {
                    ui.spinner();
                });
        }

        self.update_tray(&ctx);
        ctx.request_repaint_after(std::time::Duration::from_millis(500));
    }
}

fn load_config() -> (Config, Option<PathBuf>) {
    let path = ProjectDirs::from("org", "Codex", "PipelineWatcher")
        .map(|dirs| dirs.config_dir().join("settings.json"));
    let config: Config = path
        .as_ref()
        .and_then(|path| fs::read(path).ok())
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    (
        Config {
            poll_seconds: config.poll_seconds.clamp(10, 300),
            ..config
        },
        path,
    )
}

fn load_token() -> Option<String> {
    Entry::new(KEYRING_SERVICE, KEYRING_USER)
        .ok()?
        .get_password()
        .ok()
}

fn status_label(status: &str) -> &'static str {
    match status {
        "created" => "Created",
        "waiting_for_resource" => "Waiting for resource",
        "preparing" => "Preparing",
        "pending" => "Pending",
        "running" => "Running",
        "scheduled" => "Scheduled",
        "canceling" => "Canceling",
        "failed" => "Failed",
        "success" => "Success",
        "canceled" => "Canceled",
        "skipped" => "Skipped",
        "manual" => "Manual",
        _ => "Unknown",
    }
}

fn job_status_color(status: &str) -> Option<egui::Color32> {
    match status {
        "success" => Some(egui::Color32::LIGHT_GREEN),
        "failed" => Some(egui::Color32::LIGHT_RED),
        _ => None,
    }
}

#[cfg(windows)]
fn set_auto_start(enabled: bool) -> Result<(), String> {
    const KEY: &str = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run";
    const VALUE: &str = "PipelineWatcher";
    if !enabled
        && !Command::new("reg.exe")
            .args(["query", KEY, "/v", VALUE])
            .output()
            .map_err(|error| format!("Could not check startup settings: {error}"))?
            .status
            .success()
    {
        return Ok(());
    }

    let mut command = Command::new("reg.exe");
    if enabled {
        let path = std::env::current_exe()
            .map_err(|error| format!("Could not determine the application path: {error}"))?;
        command
            .args(["add", KEY, "/v", VALUE, "/t", "REG_SZ", "/d"])
            .arg(format!("\"{}\"", path.display()))
            .arg("/f");
    } else {
        command.args(["delete", KEY, "/v", VALUE, "/f"]);
    }
    let output = command
        .output()
        .map_err(|error| format!("Could not change startup settings: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Could not change Windows startup settings (exit code {})",
            output.status
        ))
    }
}

#[cfg(windows)]
pub fn prepare_windows_activation() -> bool {
    register_open_protocol();
    let name: Vec<u16> = OPEN_EVENT_NAME.encode_utf16().chain(Some(0)).collect();
    let event = unsafe {
        windows_sys::Win32::System::Threading::CreateEventW(std::ptr::null(), 0, 0, name.as_ptr())
    };
    if event.is_null() {
        return false;
    }
    if unsafe { windows_sys::Win32::Foundation::GetLastError() }
        == windows_sys::Win32::Foundation::ERROR_ALREADY_EXISTS
    {
        let _ = signal_running_instance();
        unsafe { windows_sys::Win32::Foundation::CloseHandle(event) };
        return true;
    }
    NOTIFICATION_OPEN_EVENT.store(event as isize, Ordering::SeqCst);
    false
}

#[cfg(windows)]
fn register_open_protocol() {
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let key = r"HKCU\Software\Classes\pipeline-watcher";
    let command = format!("\"{}\" \"%1\"", exe.display());
    let _ = Command::new("reg.exe")
        .args([
            "add",
            key,
            "/ve",
            "/d",
            "URL:Pipeline Watcher Protocol",
            "/f",
        ])
        .output();
    let _ = Command::new("reg.exe")
        .args(["add", key, "/v", "URL Protocol", "/d", "", "/f"])
        .output();
    let _ = Command::new("reg.exe")
        .args([
            "add",
            &format!("{key}\\shell\\open\\command"),
            "/ve",
            "/d",
            &command,
            "/f",
        ])
        .output();
}

#[cfg(windows)]
fn signal_running_instance() -> bool {
    let name: Vec<u16> = OPEN_EVENT_NAME.encode_utf16().chain(Some(0)).collect();
    let event = unsafe {
        windows_sys::Win32::System::Threading::OpenEventW(
            windows_sys::Win32::System::Threading::EVENT_MODIFY_STATE,
            0,
            name.as_ptr(),
        )
    };
    if event.is_null() {
        return false;
    }
    unsafe {
        use windows_sys::Win32::UI::WindowsAndMessaging::{
            AllowSetForegroundWindow, FindWindowW, GetWindowThreadProcessId,
        };
        let title: Vec<u16> = "Pipeline Watcher".encode_utf16().chain(Some(0)).collect();
        let window = FindWindowW(std::ptr::null(), title.as_ptr());
        let mut process_id = 0;
        GetWindowThreadProcessId(window, &mut process_id);
        if process_id != 0 {
            AllowSetForegroundWindow(process_id);
        }
        windows_sys::Win32::System::Threading::SetEvent(event);
        windows_sys::Win32::Foundation::CloseHandle(event);
    }
    true
}

#[cfg(windows)]
fn raise_window(hwnd: usize) {
    use windows_sys::Win32::System::Threading::{AttachThreadInput, GetCurrentThreadId};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetForegroundWindow, GetWindowThreadProcessId, HWND_TOPMOST, SW_RESTORE, SWP_NOMOVE,
        SWP_NOSIZE, SWP_SHOWWINDOW, SetForegroundWindow, SetWindowPos, ShowWindowAsync,
    };

    let hwnd = hwnd as windows_sys::Win32::Foundation::HWND;
    unsafe {
        ShowWindowAsync(hwnd, SW_RESTORE);
        let current = GetCurrentThreadId();
        let owner = GetWindowThreadProcessId(hwnd, std::ptr::null_mut());
        let foreground = GetWindowThreadProcessId(GetForegroundWindow(), std::ptr::null_mut());
        // Windows restricts foreground activation to the thread receiving user input.
        let attached = foreground != 0
            && foreground != current
            && AttachThreadInput(current, foreground, 1) != 0;
        let owner_attached =
            owner != current && owner != foreground && AttachThreadInput(current, owner, 1) != 0;
        SetWindowPos(
            hwnd,
            HWND_TOPMOST,
            0,
            0,
            0,
            0,
            SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW,
        );
        SetForegroundWindow(hwnd);
        if owner_attached {
            AttachThreadInput(current, owner, 0);
        }
        if attached {
            AttachThreadInput(current, foreground, 0);
        }
    }
}

#[cfg(windows)]
fn notify(title: &str, body: &str) {
    use windows::{
        Data::Xml::Dom::XmlDocument,
        UI::Notifications::{ToastNotification, ToastNotificationManager},
        runtime::HSTRING,
    };

    let escape = |value: &str| {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    };
    let xml = XmlDocument::new().and_then(|xml| {
        xml.LoadXml(HSTRING::from(format!(
            "<toast activationType=\"protocol\" launch=\"pipeline-watcher://open\"><visual><binding template=\"ToastGeneric\"><text>{}</text><text>{}</text></binding></visual></toast>",
            escape(title),
            escape(body)
        )))?;
        Ok(xml)
    });
    let Ok(xml) = xml else {
        return;
    };
    let Ok(toast) = ToastNotification::CreateToastNotification(xml) else {
        return;
    };
    if let Ok(notifier) = ToastNotificationManager::CreateToastNotifierWithId(HSTRING::from(
        winrt_notification::Toast::POWERSHELL_APP_ID,
    )) {
        let _ = notifier.Show(&toast);
    }
}

pub fn notify_started(project: &str, path: &str, branch: &str, id: i64) {
    let title = format!("{project}: pipeline #{id} started");
    let body = format!("{path} · branch {branch}");
    #[cfg(windows)]
    {
        notify(&title, &body);
    }
    #[cfg(not(windows))]
    {
        let _ = notify_rust::Notification::new()
            .summary(&title)
            .body(&body)
            .show();
    }
}

pub fn notify_finished(project: &str, path: &str, branch: &str, id: i64, status: &str) {
    let (state, icon) = if status == "success" {
        ("succeeded", "✅")
    } else {
        ("failed", "❌")
    };
    let title = format!("{icon} {project}: pipeline #{id} {state}");
    let body = format!("{path} · branch {branch}");
    #[cfg(windows)]
    {
        notify(&title, &body);
    }
    #[cfg(not(windows))]
    {
        let _ = notify_rust::Notification::new()
            .summary(&title)
            .body(&body)
            .show();
    }
}

#[cfg(windows)]
fn set_taskbar_overlay(hwnd: usize, count: usize) {
    use windows::{
        Win32::{
            System::Com::{CLSCTX_INPROC_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx, CoUninitialize},
            UI::{Shell::{ITaskbarList3, TaskbarList}, WindowsAndMessaging::HICON},
        },
    };

    unsafe {
        let initialized = CoInitializeEx(std::ptr::null(), COINIT_APARTMENTTHREADED).is_ok();
        let Ok(taskbar) = CoCreateInstance::<_, ITaskbarList3>(
            &TaskbarList,
            None,
            CLSCTX_INPROC_SERVER,
        ) else {
            if initialized { CoUninitialize(); }
            return;
        };
        if taskbar.HrInit().is_err() {
            if initialized { CoUninitialize(); }
            return;
        }
        let icon = if count == 0 {
            HICON::default()
        } else {
            let data = tray_icon_data(count);
            let mut resource = Vec::with_capacity(22 + 40 + data.rgba.len() + 64);
            resource.extend_from_slice(&[0, 0, 1, 0, 1, 0, 16, 16, 0, 0, 1, 0, 32, 0]);
            resource.extend_from_slice(&((40 + data.rgba.len() + 64) as u32).to_le_bytes());
            resource.extend_from_slice(&22u32.to_le_bytes());
            resource.extend_from_slice(&40u32.to_le_bytes());
            resource.extend_from_slice(&16i32.to_le_bytes());
            resource.extend_from_slice(&32i32.to_le_bytes());
            resource.extend_from_slice(&1u16.to_le_bytes());
            resource.extend_from_slice(&32u16.to_le_bytes());
            resource.extend_from_slice(&0u32.to_le_bytes());
            resource.extend_from_slice(&(data.rgba.len() as u32).to_le_bytes());
            resource.extend_from_slice(&[0; 16]);
            for y in (0..16).rev() {
                for x in 0..16 {
                    let i = (y * 16 + x) * 4;
                    resource.extend_from_slice(&[
                        data.rgba[i + 2], data.rgba[i + 1], data.rgba[i], data.rgba[i + 3],
                    ]);
                }
            }
            resource.extend_from_slice(&[0; 64]);
            let handle = windows_sys::Win32::UI::WindowsAndMessaging::CreateIconFromResourceEx(
                resource.as_ptr(), resource.len() as u32, 1, 0x0003_0000, 16, 16, 0,
            );
            HICON(handle as isize)
        };
        let description: Vec<u16> = format!("{count} running pipelines").encode_utf16().chain(Some(0)).collect();
        let _ = taskbar.SetOverlayIcon(
            windows::Win32::Foundation::HWND(hwnd as isize),
            icon,
            windows::Win32::Foundation::PWSTR(description.as_ptr() as *mut u16),
        );
        if icon.0 != 0 {
            let _ = windows::Win32::UI::WindowsAndMessaging::DestroyIcon(icon);
        }
        if initialized { CoUninitialize(); }
    }
}

fn tray_icon(count: usize) -> Icon {
    let icon = tray_icon_data(count);
    Icon::from_rgba(icon.rgba, icon.width, icon.height).expect("fixed-size tray icon")
}

fn tray_icon_data(count: usize) -> egui::IconData {
    const SIZE: usize = 16;
    let mut rgba = vec![0u8; SIZE * SIZE * 4];
    let mut pixel = |x: usize, y: usize, color: [u8; 4]| {
        let index = (y * SIZE + x) * 4;
        rgba[index..index + 4].copy_from_slice(&color);
    };
    for y in 0..SIZE {
        for x in 0..SIZE {
            let dx = x as i32 - 7;
            let dy = y as i32 - 8;
            let distance = dx * dx + dy * dy;
            if distance <= 49 {
                pixel(
                    x,
                    y,
                    if distance >= 37 {
                        [255, 255, 255, 255]
                    } else {
                        [28, 117, 214, 255]
                    },
                );
            }
        }
    }
    // A bold P stays legible at the 16 px size Windows uses in the tray.
    for y in 4..13 {
        for x in 3..5 {
            pixel(x, y, [255, 255, 255, 255]);
        }
    }
    for y in 4..9 {
        for x in 4..10 {
            if y < 6 || y >= 7 || x >= 8 {
                pixel(x, y, [255, 255, 255, 255]);
            }
        }
    }

    if count > 0 {
        let text = if count > 9 {
            vec![b'9', b'+']
        } else {
            vec![b'0' + count as u8]
        };
        for y in 8..16 {
            for x in 8..16 {
                pixel(x, y, [194, 42, 54, 255]);
            }
        }
        let glyphs: [[u8; 5]; 10] = [
            [7, 5, 5, 5, 7],
            [2, 6, 2, 2, 7],
            [7, 1, 7, 4, 7],
            [7, 1, 7, 1, 7],
            [5, 5, 7, 1, 1],
            [7, 4, 7, 1, 7],
            [7, 4, 7, 5, 7],
            [7, 1, 1, 1, 1],
            [7, 5, 7, 5, 7],
            [7, 5, 7, 1, 7],
        ];
        let width = if text.len() == 1 { 3 } else { 7 };
        let mut cursor = 8 + (8 - width) / 2;
        for character in text {
            if character.is_ascii_digit() {
                for (row, bits) in glyphs[(character - b'0') as usize].iter().enumerate() {
                    for column in 0..3 {
                        if bits & (1 << (2 - column)) != 0 {
                            pixel(cursor + column, 9 + row, [255, 255, 255, 255]);
                        }
                    }
                }
                cursor += 4;
            } else {
                for x in cursor..cursor + 3 {
                    pixel(x, 11, [255, 255, 255, 255]);
                }
                pixel(cursor + 1, 10, [255, 255, 255, 255]);
                pixel(cursor + 1, 12, [255, 255, 255, 255]);
                cursor += 3;
            }
        }
    }
    egui::IconData {
        rgba,
        width: SIZE as u32,
        height: SIZE as u32,
    }
}
