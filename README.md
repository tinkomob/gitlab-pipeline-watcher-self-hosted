# Pipeline Watcher

Desktop monitor for pipelines in GitLab Self-Managed, built with Rust and egui.

## Run

```powershell
cargo run
```

Enter the GitLab base URL (for example `https://gitlab.example.com`) and a personal or project access token with `read_api` access. The app keeps the token in the operating system credential store and saves the URL and polling interval in the user configuration directory.

The app polls all projects visible to that token and the latest 100 pipelines per project. It checks projects concurrently (up to eight requests at a time), shows active, failed, and successful pipelines, and opens GitLab links in the default browser. Stages and jobs load automatically for active pipelines; older completed pipelines load them when expanded. A `.local` DNS label is removed from the configured GitLab host and returned links. System notifications are sent when a new pipeline starts and when it succeeds or fails. Closing the window hides it to the tray; use the tray menu to open it or exit.

On Windows, enable **Start with Windows** in the connection settings to start the app for the current user at sign-in.

Temporary API and network errors trigger automatic retries with exponential backoff, up to two minutes. Per-project API errors are shown in the status area while polling continues.

TLS certificate checks remain enabled. For an internal GitLab using a private CA, install that CA in the operating system trust store before connecting.

## Notes

- Pipeline discovery uses one API request per project every 10–300 seconds. For instances with many projects, increase the polling interval.
- On Windows, native toast notifications use the Windows toast API; Windows may attribute them to PowerShell until the app is installed with a registered application identity.
- Notifications are not replayed after application restart. Pipelines already present during the first successful poll are treated as the initial baseline.
