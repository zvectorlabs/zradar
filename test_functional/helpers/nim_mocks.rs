//! Mock Prometheus exporters and an OTel-collector runner for the NIM smoke
//! test (Phase 3 R3.2).
//!
//! - [`spawn_mock_prom_exporter`] binds a tokio listener to an ephemeral port
//!   and serves a single fixed Prometheus-format response on every GET. The
//!   server runs on its own task and is dropped via [`MockExporter::shutdown`]
//!   at test end.
//! - [`render_collector_config`] templates the reference config at
//!   `examples/nemo/otel-collector-nim.yaml` with test-resolved endpoints.
//! - [`CollectorProcess`] locates the `otelcol-contrib` binary (override with
//!   the `ZRADAR_OTELCOL_BIN` env var), runs it as a child process with the
//!   rendered config, and kills it on drop.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Curated NIM/vLLM Prometheus payload covering every metric named in
/// `TECH-SPEC-PHASE-3.md §4.3`. Each metric has a single bucket / value so the
/// smoke test can assert exact `metric_name` + `metric_type` round-trip.
pub const NIM_PROM_PAYLOAD: &str = include_str!("../fixtures/nim_vllm_metrics.prom");

/// DCGM-Exporter Prometheus payload (subset matching the Phase 3 spec table).
pub const DCGM_PROM_PAYLOAD: &str = include_str!("../fixtures/dcgm_metrics.prom");

/// Handle to a running mock Prometheus exporter. Drop or call [`shutdown`] to
/// stop the listener.
pub struct MockExporter {
    pub addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl MockExporter {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for MockExporter {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn a mock Prometheus exporter that serves `payload` on every GET. Binds
/// to `127.0.0.1` on an OS-assigned ephemeral port. Returns the address as
/// `127.0.0.1:<port>`.
pub async fn spawn_mock_prom_exporter(payload: &'static str) -> Result<MockExporter> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accept = listener.accept() => {
                    let Ok((mut stream, _)) = accept else { continue };
                    tokio::spawn(async move {
                        // Read the request line + headers but discard them —
                        // any GET returns the same payload.
                        let mut buf = [0u8; 1024];
                        let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await;

                        let response = format!(
                            "HTTP/1.1 200 OK\r\n\
                             Content-Type: text/plain; version=0.0.4\r\n\
                             Content-Length: {}\r\n\
                             Connection: close\r\n\
                             \r\n\
                             {}",
                            payload.len(),
                            payload
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.shutdown().await;
                    });
                }
            }
        }
    });

    Ok(MockExporter {
        addr,
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    })
}

/// Render the reference collector YAML against the test environment so the
/// collector scrapes the mock exporters and pushes to the test zradar.
pub fn render_collector_config(
    nim_addr: &str,
    dcgm_addr: &str,
    zradar_otlp_http_url: &str,
    api_key: &str,
    workspace_id: &str,
) -> String {
    format!(
        r#"# Rendered at test time — not for production use.
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: nim-llm
          scrape_interval: 2s
          static_configs:
            - targets: ['{nim}']
              labels:
                source: nim-llm
        - job_name: dcgm
          scrape_interval: 2s
          static_configs:
            - targets: ['{dcgm}']
              labels:
                source: dcgm

processors:
  batch:
    timeout: 3s

exporters:
  otlphttp:
    endpoint: {zradar}
    tls:
      insecure: true
    headers:
      authorization: "Bearer {api_key}"
      x-workspace-id: "{workspace}"

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlphttp]
  telemetry:
    logs:
      level: warn
"#,
        nim = nim_addr,
        dcgm = dcgm_addr,
        zradar = zradar_otlp_http_url,
        api_key = api_key,
        workspace = workspace_id,
    )
}

/// Locate the `otelcol-contrib` binary. Honours `ZRADAR_OTELCOL_BIN` if set,
/// otherwise falls back to looking on `PATH`.
pub fn locate_otelcol_bin() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("ZRADAR_OTELCOL_BIN") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }
    // PATH lookup
    for name in ["otelcol-contrib", "otelcol", "otelcontribcol"] {
        if let Ok(out) = Command::new("which").arg(name).output()
            && out.status.success()
        {
            let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !path.is_empty() {
                return Some(PathBuf::from(path));
            }
        }
    }
    None
}

/// A running OTel collector child process. Killed on drop.
pub struct CollectorProcess {
    child: Option<Child>,
    pub config_path: PathBuf,
}

impl CollectorProcess {
    /// Spawn `otelcol-contrib --config <config_path>`. The config is written
    /// to a temp file rooted in `target/tmp/` to survive the test run.
    pub fn spawn(config_yaml: &str) -> Result<Self> {
        let bin = locate_otelcol_bin().ok_or_else(|| {
            anyhow!(
                "otelcol-contrib binary not found. Install from \
                 https://github.com/open-telemetry/opentelemetry-collector-releases/releases \
                 or set ZRADAR_OTELCOL_BIN."
            )
        })?;

        let tmp_dir = std::env::temp_dir().join("zradar-nim-smoke");
        std::fs::create_dir_all(&tmp_dir)?;
        let config_path = tmp_dir.join(format!("collector-{}.yaml", uuid::Uuid::new_v4().simple()));
        std::fs::write(&config_path, config_yaml)
            .with_context(|| format!("Failed to write collector config to {:?}", config_path))?;

        let child = Command::new(&bin)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("Failed to spawn {:?}", bin))?;

        Ok(Self {
            child: Some(child),
            config_path,
        })
    }
}

impl Drop for CollectorProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(&self.config_path);
    }
}

/// Returns true if the otel-collector binary is locatable on this system.
/// Tests gate their execution on this to skip cleanly in dev environments
/// where the collector is not installed.
pub fn collector_available() -> bool {
    locate_otelcol_bin().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_config_contains_required_keys() {
        let yaml = render_collector_config(
            "nim.mock:8000",
            "dcgm.mock:9400",
            "http://zradar:4318",
            "test-api-key",
            "workspace-1",
        );

        // Receivers
        assert!(yaml.contains("prometheus:"), "missing prometheus receiver");
        assert!(
            yaml.contains("nim.mock:8000"),
            "NIM target not templated in"
        );
        assert!(
            yaml.contains("dcgm.mock:9400"),
            "DCGM target not templated in"
        );

        // Processor + pipeline wiring
        assert!(yaml.contains("batch:"), "missing batch processor");
        assert!(
            yaml.contains("processors: [batch]"),
            "pipeline must reference batch processor"
        );

        // Exporter target + auth + workspace context
        assert!(
            yaml.contains("http://zradar:4318"),
            "zradar OTLP HTTP endpoint not templated in"
        );
        assert!(
            yaml.contains("\"Bearer test-api-key\""),
            "Bearer auth header not formed correctly"
        );
        assert!(
            yaml.contains("x-workspace-id: \"workspace-1\""),
            "workspace header missing"
        );
        assert!(
            yaml.contains("x-workspace-id: \"workspace-1\""),
            "workspace header missing"
        );

        // Pipeline service block
        assert!(yaml.contains("receivers: [prometheus]"));
        assert!(yaml.contains("exporters: [otlphttp]"));
    }

    #[test]
    fn rendered_config_uses_distinct_scrape_jobs() {
        // Both jobs must be present and distinguishable by job_name.
        let yaml = render_collector_config("n:1", "d:2", "http://z:4318", "k", "t");
        assert!(yaml.contains("job_name: nim-llm"));
        assert!(yaml.contains("job_name: dcgm"));
    }

    #[test]
    fn locate_otelcol_bin_respects_zradar_env_override_when_path_valid() {
        // Pick a path that always exists — `/` on unix or the current exe's parent.
        let probe = std::env::current_exe().unwrap();
        // SAFETY: tests are single-threaded by default; this env var is consumed
        // only by `locate_otelcol_bin` and only inside this test.
        unsafe { std::env::set_var("ZRADAR_OTELCOL_BIN", &probe) };
        let resolved = locate_otelcol_bin();
        unsafe { std::env::remove_var("ZRADAR_OTELCOL_BIN") };

        assert_eq!(
            resolved.as_deref(),
            Some(probe.as_path()),
            "ZRADAR_OTELCOL_BIN should be honoured when the path exists"
        );
    }

    #[test]
    fn locate_otelcol_bin_ignores_env_when_path_missing() {
        // Point at a definitely-not-real path; the function must fall through.
        let bogus = "/this/path/does/not/exist/zradar-otelcol";
        unsafe { std::env::set_var("ZRADAR_OTELCOL_BIN", bogus) };
        let resolved = locate_otelcol_bin();
        unsafe { std::env::remove_var("ZRADAR_OTELCOL_BIN") };
        // If PATH also lacks otelcol the answer is None; if a system-wide
        // otelcol exists the answer is Some(that). The only invariant we
        // can assert is that the bogus path is NOT returned.
        assert!(
            resolved.as_deref() != Some(std::path::Path::new(bogus)),
            "missing-path override must be ignored"
        );
    }

    #[test]
    fn nim_payload_covers_all_documented_metrics() {
        // The Phase 3 spec §4.3 lists 9 vllm:* metric names. Each must
        // appear in the mock fixture so the smoke test has data to assert.
        for name in [
            "vllm:num_requests_running",
            "vllm:num_requests_waiting",
            "vllm:time_to_first_token_seconds",
            "vllm:time_per_output_token_seconds",
            "vllm:e2e_request_latency_seconds",
            "vllm:kv_cache_usage_perc",
            "vllm:prompt_tokens_total",
            "vllm:generation_tokens_total",
            "vllm:request_success_total",
        ] {
            assert!(
                NIM_PROM_PAYLOAD.contains(name),
                "mock NIM payload missing {name}; smoke test will be vacuous"
            );
        }
    }

    #[test]
    fn dcgm_payload_covers_documented_metrics() {
        for name in [
            "DCGM_FI_DEV_GPU_UTIL",
            "DCGM_FI_DEV_MEM_COPY_UTIL",
            "DCGM_FI_DEV_FB_USED",
        ] {
            assert!(
                DCGM_PROM_PAYLOAD.contains(name),
                "mock DCGM payload missing {name}"
            );
        }
    }
}
