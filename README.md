# Prometheus Metrics Exporter — `dev.mcpg.observability.prometheus`

> class `metrics_sink` · `native` · package `mcpg-plugin-observability-prometheus` · artifact `libmcpg_plugin_observability_prometheus.so` · Apache-2.0

The canonical Prometheus exposition path for an MCP gateway. The plugin
accumulates the metric points the gateway's recorder emits into an in-memory
registry keyed by `(name, labels)`, and renders that registry in Prometheus
text-exposition v0.0.4 format whenever a scrape asks for it. It is a
self-contained accumulator rather than a wrapper around a global recorder, so
several metrics sinks can run in parallel and each gateway instance exposes its
own numbers. Reach for it whenever a Prometheus-compatible scraper should read
gateway metrics — it is the default metrics sink, so a factory-fresh gateway
already serves `/metrics` without any configuration.

## What it does
- Accumulates counters (sum), gauges (last value wins), and histograms (count
  and sum) per `(name, labels)` series.
- Prefixes metric names with an optional `namespace` and merges `global_labels`
  into every series; a per-sample label of the same name wins.
- Renders a `# HELP` and `# TYPE` line — plus `# UNIT` when the point carried a
  unit — ahead of each series, escaping `\`, `"`, and newlines in label values.
- Formats `NaN`, `+Inf`, and `-Inf` in the forms Prometheus expects, and prints
  whole numbers without a decimal point.
- Protects series typing: a sample whose kind disagrees with the established
  series is dropped rather than corrupting the family, and the drop is counted
  on `mcpg_prometheus_sink_kind_mismatch_total`, which is always rendered so an
  alert can fire on the zero-to-nonzero transition.
- Renders synchronously with no I/O, which is what lets the gateway serve the
  scrape endpoint straight from the accumulator.
- Declares no required capabilities — it neither opens sockets nor listens; the
  gateway's own HTTP listener serves the rendered text.

## Configuration
Referenced by id from the dedicated `observability.metrics.sinks[]` list — not
from the `plugins:` list. The plugin is compiled into the gateway binary and
registers itself when its id appears in that list and the metrics signal is on.
It is the default entry in that list, so this block is only needed to change its
settings or to run it alongside other sinks.

```yaml
observability:
  enabled: true
  metrics:
    enabled: true
    sinks:
      - kind: dev.mcpg.observability.prometheus
        config:
          namespace: mcpg          # optional metric-name prefix
          global_labels:           # added to every series
            env: production
            region: us-east-1
```

| Field | Type | Default | Description |
|---|---|---|---|
| `namespace` | string | `""` | Metric-name prefix rendered as `<namespace>_<name>`; empty disables prefixing. |
| `global_labels` | map<string,string> | `{}` | Labels merged into every series. A per-sample label of the same name takes precedence. |

Unknown fields are rejected. A `config:` block that is present but does not
parse refuses the plugin at boot rather than silently reverting to defaults; an
absent or empty block yields the defaults above.

The gateway mounts the scrape route at `/metrics` while the metrics signal is on
— both `observability.enabled` and `observability.metrics.enabled` — and the
handler renders straight from this plugin. If the plugin is not registered, that
route answers `503` rather than an empty body, so a misconfiguration is visible
to the scraper.

## Histogram rendering
Histograms are rendered as `<name>_count` and `<name>_sum` only. The plugin
folds incoming observations into those two aggregates on arrival and does not
retain individual samples, so exposition carries no bucket boundaries and
memory does not grow with observation count. Quantile-accurate histograms are a
job for a bucketed exporter wired through the recorder bridge.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-observability-prometheus --features cdylib-export --release   # → target/release/libmcpg_plugin_observability_prometheus.so
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Observability signals and how sinks fan out: <https://mcpg.dev/docs/reference/configuration>
- Plugin classes and the loading contract: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Push-based metrics over UDP instead of scraping: `libs/plugins/observability/statsd`
- The traces signal over OTLP: `libs/plugins/observability/otlp`
