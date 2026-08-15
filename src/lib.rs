//! `dev.mcpg.observability.prometheus` — pure metrics sink that
//! accumulates `MetricPoint` events into an in-memory registry and
//! renders Prometheus text-exposition format on demand.
//!
//! Implemented as a self-contained accumulator rather than wrapping
//! `metrics-exporter-prometheus`'s global recorder so that:
//!
//!   1. Operators can run multiple metrics sinks in parallel (this
//!      plugin alongside an OTLP plugin) without competing for the
//!      single global `metrics::Recorder` slot.
//!   2. The plugin's render output is per-instance, so a deployment
//!      with multiple gateway instances behind a load balancer can
//!      surface accurate per-instance metrics.
//!
//! # Wire surface
//!
//! - `emit(&MetricPoint)`: updates the registry. Counter values
//!   accumulate; Gauges replace; Histograms append observations
//!   and update count + sum.
//! - `render()`: emits the Prometheus text-exposition v0.0.4
//!   format. Synchronous — operators call this from an `http_route`
//!   plugin or the gateway's `/metrics` route handler.
//!
//! # Configuration
//!
//! ```yaml
//! observability:
//!   metrics:
//!     sinks:
//!       - kind: dev.mcpg.observability.prometheus
//!         config:
//!           namespace: mcpg          # optional metric-name prefix
//!           global_labels:           # appended to every series
//!             env: production
//!             region: us-east-1
//! ```
//!
//! # Integration with the gateway
//!
//! The gateway's metrics-rs recorder bridge intercepts
//! `counter!()` / `gauge!()` / `histogram!()` calls, converts each
//! to a [`MetricPoint`], and fans out to every registered
//! [`MetricsSink`]. This plugin is the canonical Prometheus
//! consumer of that fan-out.

use std::collections::BTreeMap;
use std::sync::Mutex;

use mcpg_plugin_protocol::{
    PluginClass, PluginManifest,
    metrics::{MetricKind, MetricPoint, MetricValue, MetricsError},
};
use mcpg_plugin_sdk::declare_plugin;
use mcpg_plugin_sdk::ffi::SyncMetricsSink;
use serde::Deserialize;

/// Plugin id — operators reference this in
/// `observability.metrics.sinks[].kind`.
pub const PLUGIN_ID: &str = "dev.mcpg.observability.prometheus";

/// Operator config schema. All fields optional with sensible
/// defaults so a `sinks: [{kind: dev.mcpg.observability.prometheus}]`
/// entry works without further configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrometheusSinkConfig {
    /// Prefix prepended to every metric name. The Prometheus
    /// convention is `<namespace>_<subsystem>_<name>`; we apply just
    /// the namespace here. Empty string disables prefixing.
    pub namespace: String,
    /// Static labels added to every series the plugin emits. Useful
    /// for tagging deployments with `env` / `region` / `cluster`.
    pub global_labels: BTreeMap<String, String>,
}

/// Identifier for one Prometheus series — name + label set.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SeriesKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl SeriesKey {
    fn new(name: String, labels: BTreeMap<String, String>) -> Self {
        // Sort labels for deterministic ordering; `BTreeMap` already
        // gives us sorted iteration but we copy into a `Vec` so the
        // key is `Hash`able for the registry's hashmap.
        let labels = labels.into_iter().collect();
        Self { name, labels }
    }
}

/// Per-series accumulator. The variant matches the `MetricKind`
/// the operator emits; mismatched re-emissions on the same name
/// (e.g. switching from counter to gauge) are tracked as a sink
/// error and the new sample is dropped to preserve the original
/// type contract.
#[derive(Debug, Clone)]
enum SeriesValue {
    Counter {
        sum_f64: f64,
        sum_i64: i64,
    },
    Gauge {
        last_f64: Option<f64>,
        last_i64: Option<i64>,
    },
    Histogram {
        count: u64,
        sum: f64,
        // NOTE: individual observations are deliberately NOT retained.
        // `render()` only emits `_count` + `_sum` (no bucket boundaries),
        // so storing every sample was unbounded memory growth for zero
        // output benefit. Incoming `MetricValue::Histogram.observations`
        // are folded into count/sum on arrival and dropped.
    },
}

impl SeriesValue {
    fn kind(&self) -> &'static str {
        match self {
            Self::Counter { .. } => "counter",
            Self::Gauge { .. } => "gauge",
            Self::Histogram { .. } => "histogram",
        }
    }

    fn merge(&mut self, value: &MetricValue) -> Result<(), &'static str> {
        match (self, value) {
            (Self::Counter { sum_f64, .. }, MetricValue::F64 { value }) => {
                *sum_f64 += *value;
                Ok(())
            }
            (Self::Counter { sum_i64, .. }, MetricValue::I64 { value }) => {
                *sum_i64 = sum_i64.saturating_add(*value);
                Ok(())
            }
            (Self::Gauge { last_f64, .. }, MetricValue::F64 { value }) => {
                *last_f64 = Some(*value);
                Ok(())
            }
            (Self::Gauge { last_i64, .. }, MetricValue::I64 { value }) => {
                *last_i64 = Some(*value);
                Ok(())
            }
            (
                Self::Histogram { count, sum },
                MetricValue::Histogram {
                    count: incoming_count,
                    sum: incoming_sum,
                    ..
                },
            ) => {
                *count = count.saturating_add(*incoming_count);
                *sum += *incoming_sum;
                Ok(())
            }
            // Operators sometimes emit a single observation as F64
            // against a histogram series; treat as a single-sample
            // addition.
            (Self::Histogram { count, sum }, MetricValue::F64 { value }) => {
                *count = count.saturating_add(1);
                *sum += *value;
                Ok(())
            }
            _ => Err("metric kind / value mismatch"),
        }
    }
}

/// One metric series in the registry — kind + accumulator. The
/// `unit` is preserved for the `# UNIT` HELP line at render time.
#[derive(Debug, Clone)]
struct Series {
    kind: MetricKind,
    unit: Option<String>,
    value: SeriesValue,
}

/// Internal mutable state the plugin owns. The Mutex is only held
/// during emit / render — both are bounded operations on small
/// data structures.
#[derive(Debug, Default)]
struct Registry {
    series: BTreeMap<SeriesKey, Series>,
    /// Count of dropped samples due to kind / value mismatch — surfaces
    /// in `render()` as a meta-metric so operators can detect bad
    /// emit patterns.
    rejected_kind_mismatch: u64,
}

pub struct PrometheusSink {
    manifest: PluginManifest,
    config: PrometheusSinkConfig,
    registry: Mutex<Registry>,
}

impl PrometheusSink {
    /// Build the plugin from operator-supplied config JSON. Fails
    /// CLOSED on a present-but-malformed config block (the factory
    /// panics, which the FFI `make` slot turns into a boot rejection)
    /// so a typo'd or schema-violating `config:` refuses the plugin
    /// rather than silently degrading to defaults. An empty / absent
    /// block (`""` / `"{}"` / `"null"`) still yields `Default`.
    pub fn from_config_json(config_json: &str) -> Self {
        let config: PrometheusSinkConfig =
            mcpg_plugin_sdk::fail_closed_config!(config_json, PrometheusSinkConfig);
        Self::with_config(config)
    }

    pub fn with_config(config: PrometheusSinkConfig) -> Self {
        Self {
            manifest: PluginManifest {
                id: PLUGIN_ID.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                name: "Prometheus Metrics Exporter".into(),
                plugin_class: PluginClass::MetricsSink,
                protocol_version: "1.0".into(),
                license: None,
                required_capabilities: Vec::new(),
                tags: Vec::new(),
                provides: Vec::new(),
                provides_schemes: Vec::new(),
                module_path_prefix: ::std::module_path!()
                    .split("::")
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                backend_profile: None,
            },
            config,
            registry: Mutex::new(Registry::default()),
        }
    }

    /// Apply operator config: prefix the metric name with the
    /// namespace + merge global labels into the per-sample labels.
    fn project(&self, metric: &MetricPoint) -> (String, BTreeMap<String, String>) {
        let name = if self.config.namespace.is_empty() {
            metric.name.clone()
        } else {
            format!("{}_{}", self.config.namespace, metric.name)
        };
        let mut labels = self.config.global_labels.clone();
        for (k, v) in &metric.labels {
            // Per-sample labels override global ones if there's a
            // collision — matches the convention every Prometheus
            // SDK uses.
            labels.insert(k.clone(), v.clone());
        }
        (name, labels)
    }

    /// Snapshot the registry as a Prometheus text-exposition v0.0.4
    /// payload. Synchronous; safe to call from a sync HTTP route
    /// handler.
    pub fn render(&self) -> String {
        let registry = self.registry.lock().expect("registry mutex poisoned");
        let mut out = String::new();

        for (key, series) in &registry.series {
            let kind_str = match series.kind {
                MetricKind::Counter => "counter",
                MetricKind::Gauge => "gauge",
                MetricKind::Histogram => "histogram",
            };
            // # HELP line — Prometheus convention is one per metric
            // family. We don't carry a description on MetricPoint
            // today, so emit a stable placeholder; the metric name
            // itself is the dominant signal.
            out.push_str(&format!("# HELP {} {} metric.\n", key.name, kind_str));
            out.push_str(&format!("# TYPE {} {}\n", key.name, kind_str));
            if let Some(unit) = &series.unit {
                out.push_str(&format!("# UNIT {} {}\n", key.name, unit));
            }

            match &series.value {
                SeriesValue::Counter { sum_f64, sum_i64 } => {
                    let value = (*sum_f64) + (*sum_i64 as f64);
                    out.push_str(&format!(
                        "{}{} {}\n",
                        key.name,
                        format_label_set(&key.labels),
                        format_float(value),
                    ));
                }
                SeriesValue::Gauge { last_f64, last_i64 } => {
                    let value =
                        last_f64.unwrap_or_else(|| last_i64.map(|v| v as f64).unwrap_or(0.0));
                    out.push_str(&format!(
                        "{}{} {}\n",
                        key.name,
                        format_label_set(&key.labels),
                        format_float(value),
                    ));
                }
                SeriesValue::Histogram { count, sum } => {
                    // Render as counter+sum since we don't carry
                    // bucket boundaries. Operators wanting bucketed
                    // histograms wire native `metrics-exporter-
                    // prometheus` via the recorder bridge.
                    out.push_str(&format!(
                        "{}_count{} {}\n",
                        key.name,
                        format_label_set(&key.labels),
                        count,
                    ));
                    out.push_str(&format!(
                        "{}_sum{} {}\n",
                        key.name,
                        format_label_set(&key.labels),
                        format_float(*sum),
                    ));
                }
            }
            out.push('\n');
        }

        // Meta-metric: count of dropped samples due to kind
        // mismatches. Always emitted even when zero so operators
        // can wire alerts on the absent-then-present transition.
        out.push_str("# HELP mcpg_prometheus_sink_kind_mismatch_total Samples dropped because the metric kind did not match the established series kind.\n");
        out.push_str("# TYPE mcpg_prometheus_sink_kind_mismatch_total counter\n");
        out.push_str(&format!(
            "mcpg_prometheus_sink_kind_mismatch_total {}\n",
            registry.rejected_kind_mismatch,
        ));

        out
    }
}

fn format_label_set(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut s = String::from("{");
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        // Per Prometheus exposition spec: label values are escaped
        // for `\n`, `"`, and `\\`.
        let escaped = v
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n");
        s.push_str(&format!("{k}=\"{escaped}\""));
    }
    s.push('}');
    s
}

/// Render a float in a Prometheus-compatible form. Integers go
/// without a decimal; non-integer values use Rust's default
/// formatter which produces `1.5e10` only for huge magnitudes.
fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value.is_infinite() {
        if value > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

impl SyncMetricsSink for PrometheusSink {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn emit(&self, metric: &MetricPoint) {
        let (name, labels) = self.project(metric);
        let key = SeriesKey::new(name, labels);
        let mut registry = self.registry.lock().expect("registry mutex poisoned");
        if registry.series.contains_key(&key) {
            // SAFETY: contains_key + get_mut is two lookups, but
            // the registry is small and hot-path uncontended; the
            // alternative is borrow-juggling around the
            // `kind_mismatch` increment below.
            let series = registry.series.get_mut(&key).expect("key just checked");
            let existing_kind_label = series.value.kind();
            let incoming_kind = metric.kind;
            if series.kind != metric.kind {
                registry.rejected_kind_mismatch = registry.rejected_kind_mismatch.saturating_add(1);
                tracing::warn!(
                    plugin_id = PLUGIN_ID,
                    metric_name = %key.name,
                    existing = existing_kind_label,
                    incoming = ?incoming_kind,
                    "metric kind mismatch on existing series — sample dropped"
                );
                return;
            }
            if series.value.merge(&metric.value).is_err() {
                registry.rejected_kind_mismatch = registry.rejected_kind_mismatch.saturating_add(1);
            }
            return;
        }

        // First sample for this series; seed the accumulator.
        let value = match (metric.kind, &metric.value) {
            (MetricKind::Counter, MetricValue::F64 { value }) => SeriesValue::Counter {
                sum_f64: *value,
                sum_i64: 0,
            },
            (MetricKind::Counter, MetricValue::I64 { value }) => SeriesValue::Counter {
                sum_f64: 0.0,
                sum_i64: *value,
            },
            (MetricKind::Gauge, MetricValue::F64 { value }) => SeriesValue::Gauge {
                last_f64: Some(*value),
                last_i64: None,
            },
            (MetricKind::Gauge, MetricValue::I64 { value }) => SeriesValue::Gauge {
                last_f64: None,
                last_i64: Some(*value),
            },
            (MetricKind::Histogram, MetricValue::Histogram { count, sum, .. }) => {
                SeriesValue::Histogram {
                    count: *count,
                    sum: *sum,
                }
            }
            (MetricKind::Histogram, MetricValue::F64 { value }) => SeriesValue::Histogram {
                count: 1,
                sum: *value,
            },
            _ => {
                registry.rejected_kind_mismatch = registry.rejected_kind_mismatch.saturating_add(1);
                return;
            }
        };

        registry.series.insert(
            key,
            Series {
                kind: metric.kind,
                unit: metric.unit.clone(),
                value,
            },
        );
    }

    fn flush(&self, _timeout_ms: u64) -> Result<(), MetricsError> {
        // No batching layer; emit() updates the registry inline,
        // and render() reads the current snapshot. Flush is a
        // no-op (returning Ok matches the trait's default).
        Ok(())
    }

    fn render_text_exposition(&self) -> Option<String> {
        // The gateway's `/metrics` route pulls through this slot via
        // the metrics-rs recorder bridge. Render from the in-memory
        // accumulator — synchronous, no IO.
        Some(self.render())
    }
}

declare_plugin! {
    plugin_id: PLUGIN_ID,
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[],
    entities: [
        metrics_sink as entity {
            inner_name: "",
            plugin_type: PrometheusSink,
            factory: |cfg, _host: ::mcpg_plugin_sdk::HostHandle| PrometheusSink::from_config_json(cfg),
        }
    ],
}

// ---------------------------------------------------------------------------
// Async trait bridge
// ---------------------------------------------------------------------------
//
// The macro wires the cdylib FFI surface against the SDK's
// `SyncMetricsSink`. For first-party static linking (the gateway
// registers the plugin via [`FirstPartyRegistrar`]), we also need
// the async [`mcpg_plugin_protocol::metrics::MetricsSink`] impl.
// Both surfaces forward to the same internal accumulator; the
// async methods are sync-as-async (no await) since the plugin's
// state is in a `parking_lot::Mutex`-equivalent (`std::sync::
// Mutex`) and contention is bounded.

#[mcpg_plugin_protocol::async_trait]
impl mcpg_plugin_protocol::metrics::MetricsSink for PrometheusSink {
    fn manifest(&self) -> &PluginManifest {
        <Self as SyncMetricsSink>::manifest(self)
    }

    async fn emit(&self, metric: &MetricPoint) {
        <Self as SyncMetricsSink>::emit(self, metric);
    }

    async fn flush(&self, timeout: std::time::Duration) -> Result<(), MetricsError> {
        let timeout_ms = timeout.as_millis().min(u64::MAX as u128) as u64;
        <Self as SyncMetricsSink>::flush(self, timeout_ms)
    }

    async fn render_text_exposition(&self) -> Option<String> {
        <Self as SyncMetricsSink>::render_text_exposition(self)
    }

    async fn shutdown(&self) {
        <Self as SyncMetricsSink>::shutdown(self);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn point_counter(name: &str, labels: &[(&str, &str)], value: i64) -> MetricPoint {
        MetricPoint {
            name: name.into(),
            unit: None,
            kind: MetricKind::Counter,
            value: MetricValue::I64 { value },
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
            timestamp_ns: 0,
        }
    }

    fn point_gauge_f64(name: &str, value: f64) -> MetricPoint {
        MetricPoint {
            name: name.into(),
            unit: None,
            kind: MetricKind::Gauge,
            value: MetricValue::F64 { value },
            labels: BTreeMap::new(),
            timestamp_ns: 0,
        }
    }

    #[test]
    fn manifest_carries_metrics_sink_class() {
        let sink = PrometheusSink::with_config(Default::default());
        assert_eq!(sink.manifest().id, PLUGIN_ID);
        assert_eq!(sink.manifest().plugin_class, PluginClass::MetricsSink);
        assert_eq!(sink.manifest().protocol_version, "1.0");
    }

    #[test]
    fn config_default_yields_empty_namespace_and_labels() {
        let cfg: PrometheusSinkConfig = serde_json::from_str("{}").unwrap();
        assert!(cfg.namespace.is_empty());
        assert!(cfg.global_labels.is_empty());
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let res = serde_json::from_str::<PrometheusSinkConfig>(r#"{"unknown": 1}"#);
        assert!(res.is_err(), "deny_unknown_fields should reject typos");
    }

    #[test]
    #[should_panic(expected = "failing closed")]
    fn from_config_json_fails_closed_on_malformed() {
        // A present-but-malformed config block must refuse the plugin
        // (fail CLOSED) rather than silently degrading to defaults.
        let _ = PrometheusSink::from_config_json("not json");
    }

    #[test]
    fn from_config_json_empty_block_yields_defaults() {
        // An empty / absent / unit config block is an opt-out, not a
        // typo — it still yields a working sink with default config.
        for empty in ["", "{}", "null"] {
            let sink = PrometheusSink::from_config_json(empty);
            assert!(
                sink.config.namespace.is_empty(),
                "empty config {empty:?} should yield default namespace"
            );
            assert!(
                sink.config.global_labels.is_empty(),
                "empty config {empty:?} should yield default labels"
            );
        }
    }

    #[test]
    fn counter_accumulates_across_samples() {
        let sink = PrometheusSink::with_config(Default::default());
        sink.emit(&point_counter("requests_total", &[], 1));
        sink.emit(&point_counter("requests_total", &[], 4));
        let out = sink.render();
        assert!(
            out.contains("requests_total 5"),
            "counter should accumulate; got:\n{out}"
        );
    }

    #[test]
    fn gauge_replaces_with_latest_sample() {
        let sink = PrometheusSink::with_config(Default::default());
        sink.emit(&point_gauge_f64("active_sessions", 3.0));
        sink.emit(&point_gauge_f64("active_sessions", 7.5));
        let out = sink.render();
        assert!(
            out.contains("active_sessions 7.5"),
            "gauge should reflect last sample; got:\n{out}"
        );
        assert!(
            !out.contains("active_sessions 3"),
            "gauge should not retain prior sample; got:\n{out}"
        );
    }

    #[test]
    fn labels_render_in_braces_and_escape_special_chars() {
        let sink = PrometheusSink::with_config(Default::default());
        sink.emit(&point_counter(
            "tool_calls_total",
            &[("tool", r#"weird "name""#), ("env", "prod")],
            1,
        ));
        let out = sink.render();
        assert!(
            out.contains(r#"tool_calls_total{env="prod",tool="weird \"name\""} 1"#),
            "labels should be quoted + escaped; got:\n{out}"
        );
    }

    #[test]
    fn namespace_prefixes_metric_names() {
        let sink = PrometheusSink::with_config(PrometheusSinkConfig {
            namespace: "mcpg".into(),
            global_labels: BTreeMap::new(),
        });
        sink.emit(&point_counter("requests_total", &[], 1));
        let out = sink.render();
        assert!(
            out.contains("mcpg_requests_total 1"),
            "namespace should be prepended; got:\n{out}"
        );
    }

    #[test]
    fn global_labels_added_to_every_series() {
        let sink = PrometheusSink::with_config(PrometheusSinkConfig {
            namespace: String::new(),
            global_labels: BTreeMap::from([("env".into(), "prod".into())]),
        });
        sink.emit(&point_counter("up", &[], 1));
        let out = sink.render();
        assert!(
            out.contains(r#"up{env="prod"} 1"#),
            "global label should be applied; got:\n{out}"
        );
    }

    #[test]
    fn per_sample_labels_override_global_labels() {
        let sink = PrometheusSink::with_config(PrometheusSinkConfig {
            namespace: String::new(),
            global_labels: BTreeMap::from([("env".into(), "global".into())]),
        });
        sink.emit(&point_counter("up", &[("env", "override")], 1));
        let out = sink.render();
        assert!(
            out.contains(r#"up{env="override"} 1"#),
            "per-sample label should win; got:\n{out}"
        );
    }

    #[test]
    fn kind_mismatch_drops_sample_and_increments_meta() {
        let sink = PrometheusSink::with_config(Default::default());
        sink.emit(&point_counter("flips", &[], 1));
        // Now emit a Gauge to the same series — should be rejected.
        sink.emit(&point_gauge_f64("flips", 99.0));
        let out = sink.render();
        assert!(
            out.contains("flips 1"),
            "original counter sample should still be in the output; got:\n{out}"
        );
        assert!(
            !out.contains("99"),
            "gauge sample on counter series should be dropped; got:\n{out}"
        );
        assert!(
            out.contains("mcpg_prometheus_sink_kind_mismatch_total 1"),
            "meta-metric should record the rejection; got:\n{out}"
        );
    }

    #[test]
    fn render_emits_help_and_type_lines() {
        let sink = PrometheusSink::with_config(Default::default());
        sink.emit(&point_counter("ops", &[], 1));
        let out = sink.render();
        assert!(out.contains("# HELP ops counter metric."));
        assert!(out.contains("# TYPE ops counter"));
    }

    #[test]
    fn histogram_accumulates_count_and_sum() {
        let sink = PrometheusSink::with_config(Default::default());
        let m = MetricPoint {
            name: "request_ms".into(),
            unit: Some("ms".into()),
            kind: MetricKind::Histogram,
            value: MetricValue::Histogram {
                count: 2,
                sum: 30.0,
                observations: vec![10.0, 20.0],
            },
            labels: BTreeMap::new(),
            timestamp_ns: 0,
        };
        sink.emit(&m);
        sink.emit(&m);
        let out = sink.render();
        assert!(
            out.contains("request_ms_count 4"),
            "histogram counts merge; got:\n{out}"
        );
        assert!(
            out.contains("request_ms_sum 60"),
            "histogram sums merge; got:\n{out}"
        );
    }

    #[test]
    fn flush_is_noop_ok() {
        let sink = PrometheusSink::with_config(Default::default());
        assert!(sink.flush(0).is_ok());
    }

    #[test]
    fn render_text_exposition_returns_some_payload_after_emit() {
        // The gateway's `/metrics` route pulls the text snapshot
        // through the `MetricsSink::render_text_exposition` slot.
        // A Prometheus plugin must
        // surface `Some(text)` even pre-emit (the meta-metric
        // line is always present), and the payload must include
        // the emitted series after at least one sample.
        let sink = PrometheusSink::with_config(Default::default());
        let pre = SyncMetricsSink::render_text_exposition(&sink)
            .expect("Prometheus sink renders empty snapshot pre-emit");
        assert!(
            pre.contains("mcpg_prometheus_sink_kind_mismatch_total"),
            "meta-metric should be present pre-emit; got:\n{pre}"
        );
        sink.emit(&point_counter("requests_total", &[], 1));
        let post =
            SyncMetricsSink::render_text_exposition(&sink).expect("post-emit snapshot is Some");
        assert!(
            post.contains("requests_total 1"),
            "render_text_exposition should include the emitted sample; got:\n{post}"
        );
    }

    #[test]
    fn descriptor_yaml_id_matches_plugin_id_const() {
        assert!(
            DESCRIPTOR_YAML.contains(&format!("id: {PLUGIN_ID}")),
            "descriptor YAML id should match PLUGIN_ID const"
        );
        assert!(
            DESCRIPTOR_YAML.contains("class: metrics_sink"),
            "descriptor YAML class should be metrics_sink"
        );
    }
}
