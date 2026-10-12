use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use reqwest::{Client, Url, header};
use serde::Deserialize;
use tokio::sync::{Semaphore, mpsc as tokio_mpsc, watch};

use crate::app::{Config, Pipeline, PipelineJob, PollResult, WorkerCommand, WorkerUpdate};

#[derive(Deserialize)]
struct Project {
    id: i64,
    name: String,
    path_with_namespace: String,
}

#[derive(Deserialize)]
struct ApiPipeline {
    id: i64,
    status: String,
    #[serde(default)]
    r#ref: String,
    #[serde(default)]
    web_url: String,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    updated_at: String,
}

#[derive(Deserialize)]
struct ApiJob {
    name: String,
    stage: String,
    status: String,
    #[serde(default)]
    web_url: String,
}

pub fn start(
    config_rx: watch::Receiver<Config>,
    command_rx: tokio_mpsc::UnboundedReceiver<WorkerCommand>,
    tx: std::sync::mpsc::Sender<WorkerUpdate>,
    ctx: eframe::egui::Context,
) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        let Ok(runtime) = runtime else {
            let _ = tx.send(WorkerUpdate::Snapshot(PollResult::error(
                "Could not start the background worker",
            )));
            return;
        };
        runtime.block_on(run(config_rx, command_rx, tx, ctx));
    });
}

async fn run(
    mut config_rx: watch::Receiver<Config>,
    mut command_rx: tokio_mpsc::UnboundedReceiver<WorkerCommand>,
    tx: std::sync::mpsc::Sender<WorkerUpdate>,
    ctx: eframe::egui::Context,
) {
    let mut seen: HashMap<(i64, i64), String> = HashMap::new();
    let mut baseline_ready = false;
    let mut retry_seconds = 5u64;
    let mut current_config = config_rx.borrow().clone();
    let jobs_semaphore = Arc::new(Semaphore::new(8));

    loop {
        if !current_config.is_ready() {
            let _ = tx.send(WorkerUpdate::Snapshot(PollResult::error(
                "Enter your GitLab URL and a token with read_api access",
            )));
            ctx.request_repaint();
            if config_rx.changed().await.is_err() {
                return;
            }
            current_config = config_rx.borrow().clone();
            baseline_ready = false;
            seen.clear();
            retry_seconds = 5;
            continue;
        }

        let mut result = poll(&current_config, &mut seen, baseline_ready).await;
        baseline_ready |= result.updated_at.is_some();
        let has_errors = !result.error.is_empty();
        let wait_seconds = if !has_errors {
            retry_seconds = 5;
            current_config.poll_seconds.clamp(10, 300)
        } else {
            let delay = retry_seconds;
            retry_seconds = (retry_seconds * 2).min(120);
            if result.updated_at.is_none() {
                result.status = format!("Disconnected — retrying in {delay}s");
            } else {
                result.status = format!("API errors — retrying in {delay}s");
            }
            delay
        };
        let _ = tx.send(WorkerUpdate::Snapshot(result));
        ctx.request_repaint();

        let sleep = tokio::time::sleep(Duration::from_secs(wait_seconds));
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                changed = config_rx.changed() => {
                    if changed.is_err() { return; }
                    baseline_ready = false;
                    seen.clear();
                    retry_seconds = 5;
                    break;
                }
                command = command_rx.recv() => {
                    let Some(command) = command else { return; };
                    let config = current_config.clone();
                    let tx = tx.clone();
                    let ctx = ctx.clone();
                    let semaphore = jobs_semaphore.clone();
                    tokio::spawn(async move {
                        if let Ok(_permit) = semaphore.acquire_owned().await {
                            handle_command(&config, command, &tx, &ctx).await;
                        }
                    });
                }
            }
        }
        current_config = config_rx.borrow().clone();
    }
}

async fn handle_command(
    config: &Config,
    command: WorkerCommand,
    tx: &std::sync::mpsc::Sender<WorkerUpdate>,
    ctx: &eframe::egui::Context,
) {
    match command {
        WorkerCommand::FetchJobs {
            project_id,
            pipeline_id,
        } => {
            let result = get_pipeline_jobs(config, project_id, pipeline_id).await;
            let _ = tx.send(WorkerUpdate::Jobs {
                project_id,
                pipeline_id,
                result,
            });
            ctx.request_repaint();
        }
    }
}

async fn poll(
    config: &Config,
    seen: &mut HashMap<(i64, i64), String>,
    notify_transitions: bool,
) -> PollResult {
    let (client, api_base) = match client_and_api(config) {
        Ok(value) => value,
        Err(error) => return PollResult::error(&error),
    };

    let projects = match get_projects(&client, &api_base).await {
        Ok(projects) => projects,
        Err(error) => return PollResult::error(&error),
    };

    let mut fetched: Vec<(Project, Result<Vec<ApiPipeline>, String>)> = stream::iter(projects)
        .map(|project| {
            let client = client.clone();
            let api_base = api_base.clone();
            async move {
                let result = get_pipelines(&client, &api_base, project.id).await;
                (project, result)
            }
        })
        .buffer_unordered(8)
        .collect()
        .await;
    fetched.sort_by(|a, b| a.0.path_with_namespace.cmp(&b.0.path_with_namespace));

    let mut pipelines = Vec::new();
    let mut errors = Vec::new();
    for (project, result) in fetched {
        match result {
            Ok(items) => {
                for item in items {
                    let key = (project.id, item.id);
                    let previous = seen.get(&key).cloned();
                    let is_active = Pipeline::is_active_status(&item.status);
                    let is_terminal = matches!(item.status.as_str(), "success" | "failed");
                    if notify_transitions {
                        match previous.as_deref() {
                            None if is_active => super::app::notify_started(
                                &project.name,
                                &project.path_with_namespace,
                                &item.r#ref,
                                item.id,
                            ),
                            None if is_terminal => super::app::notify_finished(
                                &project.name,
                                &project.path_with_namespace,
                                &item.r#ref,
                                item.id,
                                &item.status,
                            ),
                            Some(previous)
                                if !Pipeline::is_active_status(previous) && is_active =>
                            {
                                super::app::notify_started(
                                    &project.name,
                                    &project.path_with_namespace,
                                    &item.r#ref,
                                    item.id,
                                )
                            }
                            Some(previous) if previous != item.status && is_terminal => {
                                super::app::notify_finished(
                                    &project.name,
                                    &project.path_with_namespace,
                                    &item.r#ref,
                                    item.id,
                                    &item.status,
                                )
                            }
                            _ => {}
                        }
                    }
                    seen.insert(key, item.status.clone());
                    pipelines.push(Pipeline {
                        project_id: project.id,
                        path: project.path_with_namespace.clone(),
                        id: item.id,
                        status: item.status,
                        branch: item.r#ref,
                        url: without_local_host_label(&item.web_url),
                        updated_at: if item.updated_at.is_empty() {
                            item.created_at
                        } else {
                            item.updated_at
                        },
                    });
                }
            }
            Err(error) => errors.push(format!("{}: {}", project.path_with_namespace, error)),
        }
    }

    PollResult {
        pipelines,
        status: if errors.is_empty() {
            "Connected".into()
        } else {
            format!("Connected, project errors: {}", errors.len())
        },
        error: errors.into_iter().take(3).collect::<Vec<_>>().join("\n"),
        updated_at: Some(std::time::SystemTime::now()),
    }
}

fn client_and_api(config: &Config) -> Result<(Client, String), String> {
    let normalized_url = without_local_host_label(config.base_url.trim().trim_end_matches('/'));
    let base = Url::parse(&normalized_url)
        .map_err(|_| "Invalid GitLab URL. Use http:// or https://".to_owned())?;
    if !matches!(base.scheme(), "http" | "https") {
        return Err("GitLab URL must start with http:// or https://".into());
    }
    if base.host().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err(
            "Enter a GitLab URL without credentials, query parameters, or a fragment".into(),
        );
    }
    let api_base = format!("{}/api/v4/", base.as_str().trim_end_matches('/'));
    let token = config
        .token
        .parse()
        .map_err(|_| "The token contains invalid characters".to_owned())?;
    let mut headers = header::HeaderMap::new();
    headers.insert("PRIVATE-TOKEN", token);
    let client = Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|_| "Could not create the HTTP client".to_owned())?;
    Ok((client, api_base))
}

pub fn without_local_host_label(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return value.to_owned();
    };
    let Some(host) = url.host_str() else {
        return value.to_owned();
    };
    if !host
        .split('.')
        .any(|label| label.eq_ignore_ascii_case("local"))
    {
        return value.to_owned();
    }
    let host = host
        .split('.')
        .filter(|label| !label.eq_ignore_ascii_case("local"))
        .collect::<Vec<_>>()
        .join(".");
    if url.set_host(Some(&host)).is_err() {
        return value.to_owned();
    }
    url.to_string()
}

fn request_error(error: reqwest::Error) -> String {
    if error.is_timeout() {
        "GitLab connection timed out".into()
    } else if let Some(status) = error.status() {
        format!("GitLab returned HTTP {status}")
    } else {
        "Network error while contacting GitLab".into()
    }
}

async fn get_projects(client: &Client, api_base: &str) -> Result<Vec<Project>, String> {
    let mut projects = Vec::new();
    let mut page = 1;
    loop {
        let response = client
            .get(format!("{api_base}projects"))
            .query(&[
                ("simple", "true"),
                ("per_page", "100"),
                ("page", &page.to_string()),
            ])
            .send()
            .await
            .map_err(request_error)?
            .error_for_status()
            .map_err(request_error)?;
        let next_page = response
            .headers()
            .get("x-next-page")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let mut batch: Vec<Project> = response
            .json()
            .await
            .map_err(|_| "GitLab returned an invalid project list".to_owned())?;
        if batch.is_empty() {
            break;
        }
        projects.append(&mut batch);
        let Ok(next) = next_page.parse::<usize>() else {
            break;
        };
        page = next;
    }
    Ok(projects)
}

async fn get_pipelines(
    client: &Client,
    api_base: &str,
    project_id: i64,
) -> Result<Vec<ApiPipeline>, String> {
    let response = client
        .get(format!("{api_base}projects/{project_id}/pipelines"))
        .query(&[("per_page", "100"), ("order_by", "id"), ("sort", "desc")])
        .send()
        .await
        .map_err(request_error)?
        .error_for_status()
        .map_err(request_error)?;
    response
        .json()
        .await
        .map_err(|_| "GitLab returned an invalid pipeline list".to_owned())
}

async fn get_pipeline_jobs(
    config: &Config,
    project_id: i64,
    pipeline_id: i64,
) -> Result<Vec<PipelineJob>, String> {
    let (client, api_base) = client_and_api(config)?;
    let mut jobs = Vec::new();
    let mut page = 1;
    loop {
        let response = client
            .get(format!(
                "{api_base}projects/{project_id}/pipelines/{pipeline_id}/jobs"
            ))
            .query(&[("per_page", "100"), ("page", &page.to_string())])
            .send()
            .await
            .map_err(request_error)?
            .error_for_status()
            .map_err(request_error)?;
        let next_page = response
            .headers()
            .get("x-next-page")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let mut batch: Vec<ApiJob> = response
            .json()
            .await
            .map_err(|_| "GitLab returned an invalid job list".to_owned())?;
        if batch.is_empty() {
            break;
        }
        jobs.extend(batch.drain(..).map(|job| PipelineJob {
            stage: job.stage,
            name: job.name,
            status: job.status,
            url: without_local_host_label(&job.web_url),
        }));
        let Ok(next) = next_page.parse::<usize>() else {
            break;
        };
        page = next;
    }
    jobs.reverse();
    Ok(jobs)
}
