#![windows_subsystem = "windows"]

mod app;
mod gitlab;

fn main() -> eframe::Result {
    #[cfg(windows)]
    if app::prepare_windows_activation() {
        return Ok(());
    }

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([920.0, 640.0])
            .with_min_inner_size([720.0, 480.0])
            .with_title("Pipeline Watcher"),
        ..Default::default()
    };

    eframe::run_native(
        "Pipeline Watcher",
        options,
        Box::new(|cc| Ok(Box::new(app::PipelineWatcher::new(cc)))),
    )
}
