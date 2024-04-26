use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use http::Uri;
use itertools::Itertools;
use opentelemetry::logs::{AnyValue, Severity};
use opentelemetry::trace::SpanId;
use opentelemetry::trace::{Status, TraceError};
use opentelemetry::Key;
use opentelemetry_sdk::export::logs::LogData;
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::resource::ResourceDetector;
use opentelemetry_sdk::resource::SdkProvidedResourceDetector;
use opentelemetry_semantic_conventions as semcov;
use prost::Message;
use rand::Rng;
#[cfg(not(feature = "reqwest-client"))]
use reqwest as _;
use reqwest::{Client, RequestBuilder};
use send_wrapper::SendWrapper;

pub use model::Error;

use crate::dd_proto;

#[cfg(target_arch = "wasm32")]
use getrandom as _;

mod model;

const DEFAULT_TRACING_ENDPOINT: &str = "https://trace.agent.datadoghq.eu/";
const DEFAULT_LOGS_INTAKE_ENDPOINT: &str = "https://http-intake.logs.datadoghq.com/";
const DEFAULT_DD_TRACES_PATH: &str = "api/v0.2/traces";
const DEFAULT_DD_STATS_PATH: &str = "api/v0.2/stats";
const DEFAULT_DD_LOGS_PATH: &str = "api/v2/logs";
const TRACES_DD_CONTENT_TYPE: &str = "application/x-protobuf";
const STATS_DD_CONTENT_TYPE: &str = "application/msgpack";
const LOGS_DD_CONTENT_TYPE: &str = "application/json";
const DEFAULT_DD_API_KEY_HEADER: &str = "DD-Api-Key";

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum DatadogExportError {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("http error: {0}")]
    Http(http::StatusCode, String),
}

/// Datadog span exporter
#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct DatadogExporter {
    client: Arc<Client>,
    trace_request_url: Uri,
    stats_request_url: Uri,
    logs_request_url: Uri,
    service_name: String,
    env: String,
    tags: BTreeMap<String, String>,
    host_name: String,
    key: String,
    runtime_id: String,
    container_id: String,
    app_version: String,
    sampling: u8,
}

impl DatadogExporter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        service_name: String,
        logs_request_url: Uri,
        stats_request_url: Uri,
        trace_request_url: Uri,
        client: Arc<Client>,
        key: String,
        env: String,
        tags: BTreeMap<String, String>,
        host_name: String,
        runtime_id: String,
        container_id: String,
        app_version: String,
        sampling: u8,
    ) -> Self {
        DatadogExporter {
            client,
            logs_request_url,
            stats_request_url,
            trace_request_url,
            service_name,
            env,
            tags,
            host_name,
            key,
            runtime_id,
            container_id,
            app_version,
            sampling,
        }
    }
}

/// Create a new Datadog exporter pipeline builder.
#[must_use]
pub fn new_pipeline() -> DatadogPipelineBuilder {
    DatadogPipelineBuilder::default()
}

/// Builder for `ExporterConfig` struct.
#[derive(Debug)]
pub struct DatadogPipelineBuilder {
    trace_endpoint: String,
    logs_endpoint: String,
    sampling: u8,
    api_key: Option<String>,
    app_version: Option<String>,
    client: Option<Arc<Client>>,
    container_id: Option<String>,
    env: Option<String>,
    host_name: Option<String>,
    runtime_id: Option<String>,
    service_name: Option<String>,
    tags: Option<BTreeMap<String, String>>,
}

impl Default for DatadogPipelineBuilder {
    fn default() -> Self {
        DatadogPipelineBuilder {
            trace_endpoint: DEFAULT_TRACING_ENDPOINT.to_string(),
            logs_endpoint: DEFAULT_LOGS_INTAKE_ENDPOINT.to_string(),
            service_name: None,
            api_key: None,
            #[cfg(not(feature = "reqwest-client"))]
            client: None,
            #[cfg(feature = "reqwest-client")]
            client: Some(Arc::new(reqwest::Client::new())),
            env: None,
            tags: None,
            host_name: None,
            runtime_id: None,
            container_id: None,
            app_version: None,
            sampling: 100,
        }
    }
}

/// A [`SpanProcessor`] that exports asynchronously when asked to do it.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct WASMWorkerSpanProcessor {
    exporter: DatadogExporter,
    flush_size: usize,
}

impl WASMWorkerSpanProcessor {
    pub(crate) fn new(exporter: DatadogExporter, flush_size: usize) -> Self {
        WASMWorkerSpanProcessor {
            exporter,
            flush_size,
        }
    }
}

#[async_trait]
pub trait SpanProcessExt {
    async fn force_flush(&self) -> TraceResult<()>;
}

#[async_trait]
impl SpanProcessExt for WASMWorkerSpanProcessor {
    async fn force_flush(&self) -> TraceResult<()> {
        let to_export = {
            SPANS.with(|spans| {
                let mut spans = spans
                    .try_borrow_mut()
                    .expect("should safely succeeded given the single threaded runtime");

                let export_size = if spans.len() > self.flush_size {
                    self.flush_size
                } else {
                    spans.len()
                };

                spans.drain(0..export_size).collect::<Vec<_>>()
            })
        };

        self.exporter.export(to_export).await
    }
}

impl SpanProcessor for WASMWorkerSpanProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &opentelemetry::Context) {
        // Ignored
    }

    fn on_end(&self, span: SpanData) {
        SPANS.with(|spans| {
            spans
                .try_borrow_mut()
                .expect("should safely succeeded given the single threaded runtime")
                .push(span);
        });
    }

    fn force_flush(&self) -> TraceResult<()> {
        Err(TraceError::from(
            "Sync flush is not supported, use `force_flush` from `SpanProcessExt`",
        ))
    }

    fn shutdown(&mut self) -> TraceResult<()> {
        // We ignore the Shutdown as we are in a Worker process, either it'll be shutdown by the
        // worker termination or it'll keep existing.
        //
        // TODO: Better handle it later.
        Ok(())
    }
}

impl DatadogPipelineBuilder {
    /// Building a new exporter.
    ///
    /// This is useful if you are manually constructing a pipeline.
    ///
    /// # Errors
    ///
    /// If the Endpoint or the `APIKey` are not properly set.
    pub fn build_exporter(mut self) -> Result<DatadogExporter, TraceError> {
        let service_name = self.service_name();
        self.build_exporter_with_service_name(service_name)
    }

    fn service_name(&mut self) -> String {
        let service_name = self.service_name.take();

        service_name.unwrap_or_else(|| {
            SdkProvidedResourceDetector
                .detect(Duration::from_secs(0))
                .get(Key::new(semcov::resource::SERVICE_NAME.to_string()))
                .unwrap()
                .to_string()
        })
    }

    fn build_exporter_with_service_name(
        self,
        service_name: String,
    ) -> Result<DatadogExporter, TraceError> {
        if let Some(client) = self.client {
            let traces_endpoint = self.trace_endpoint.clone() + DEFAULT_DD_TRACES_PATH;
            let stats_endpoint = self.trace_endpoint.clone() + DEFAULT_DD_STATS_PATH;
            let logs_endpoint = self.logs_endpoint.clone() + DEFAULT_DD_LOGS_PATH;

            let exporter = DatadogExporter::new(
                service_name,
                logs_endpoint.parse().map_err::<Error, _>(Into::into)?,
                stats_endpoint.parse().map_err::<Error, _>(Into::into)?,
                traces_endpoint.parse().map_err::<Error, _>(Into::into)?,
                client,
                self.api_key
                    .ok_or_else(|| TraceError::Other("APIKey not provided".into()))?,
                self.env.unwrap_or_default(),
                self.tags.unwrap_or_default(),
                self.host_name.unwrap_or_default(),
                self.runtime_id.unwrap_or_default(),
                self.container_id.unwrap_or_default(),
                self.app_version.unwrap_or_default(),
                self.sampling,
            );
            Ok(exporter)
        } else {
            Err(Error::NoHttpClient.into())
        }
    }

    /// Assign the service name under which to group traces
    #[must_use]
    pub fn with_service_name<T: Into<String>>(mut self, name: T) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Assign the Datadog trace endpoint
    #[must_use]
    pub fn with_tracing_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.trace_endpoint = endpoint.into();
        self
    }

    /// Assign the Datadog logs endpoint
    #[must_use]
    pub fn with_logs_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.logs_endpoint = endpoint.into();
        self
    }

    #[must_use]
    pub fn with_api_key<T: Into<String>>(mut self, key: Option<T>) -> Self {
        self.api_key = key.map(Into::into);
        self
    }

    /// Choose the http client used by uploader
    #[must_use]
    pub fn with_http_client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Assign the env
    #[must_use]
    pub fn with_env(mut self, env: String) -> Self {
        self.env = Some(env);
        self
    }

    /// Assign the `host_name`
    #[must_use]
    pub fn with_host_name(mut self, host_name: String) -> Self {
        self.host_name = Some(host_name);
        self
    }

    /// Assign the `runtime_id`
    #[must_use]
    pub fn with_runtime_id(mut self, runtime_id: String) -> Self {
        self.runtime_id = Some(runtime_id);
        self
    }

    /// Assign the `container_id`
    #[must_use]
    pub fn with_container_id(mut self, container_id: String) -> Self {
        self.container_id = Some(container_id);
        self
    }

    /// Assign the `app_version`
    #[must_use]
    pub fn with_app_version(mut self, app_version: String) -> Self {
        self.app_version = Some(app_version);
        self
    }

    /// Assign the tags
    #[must_use]
    pub fn with_tags(mut self, tags: BTreeMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Assign the Datadog trace endpoint
    #[must_use]
    pub fn with_sampling(mut self, mut sampling: u8) -> Self {
        if sampling > 100 {
            sampling = 100;
        }

        self.sampling = sampling;
        self
    }
}

fn group_into_traces(spans: Vec<SpanData>, sampling: u8) -> Vec<Vec<SpanData>> {
    spans
        .into_iter()
        .into_group_map_by(|span_data| span_data.span_context.trace_id())
        .into_values()
        .filter(|_| {
            let random = rand::thread_rng().gen_range(0..100);
            random <= sampling
        })
        .collect()
}

/// Helper function whish should be rewritte, as we only need u64 for `TraceID`
pub(crate) fn u128_to_u64s(n: u128) -> [u64; 2] {
    let bytes = n.to_ne_bytes();
    let (mut high, mut low) = bytes.split_at(8);

    if cfg!(target_endian = "little") {
        std::mem::swap(&mut high, &mut low);
    }

    [
        u64::from_ne_bytes(high.try_into().unwrap()),
        u64::from_ne_bytes(low.try_into().unwrap()),
    ]
}

fn otel_span_to_dd_span(exporter: &DatadogExporter, span: SpanData) -> dd_proto::Span {
    let trace_id = span.span_context.trace_id();
    let span_id: SpanId = span.span_context.span_id();
    let span_id = u64::from_be_bytes(span_id.to_bytes());
    let parent_id = span.parent_span_id;
    let parent_id = u64::from_be_bytes(parent_id.to_bytes());

    let span_name = span.name;
    let [t0, _t1] = u128_to_u64s(u128::from_be_bytes(trace_id.to_bytes()));

    #[allow(clippy::cast_possible_truncation)]
    let start = span
        .start_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let duration = span
        .end_time
        .duration_since(span.start_time)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut meta = span
        .attributes
        .into_iter()
        .map(|key_value| (key_value.key.to_string(), key_value.value.to_string()))
        .collect::<BTreeMap<String, String>>();

    meta.extend(exporter.tags.clone());
    meta.insert("dd_service".to_string(), exporter.service_name.clone());

    dd_proto::Span {
        service: exporter.service_name.clone(),
        name: span_name.to_string(),
        resource: span_name.to_string(),
        r#type: "http".to_string(),
        trace_id: t0,
        span_id,
        parent_id,
        start,
        duration,
        error: match span.status {
            Status::Unset | Status::Ok => 0,
            Status::Error { .. } => 1,
        },
        meta,
        metrics: BTreeMap::from_iter([
            ("_dd.measured".to_string(), 1.0),
            ("_sampling_priority_v1".to_string(), 1.0),
        ]),
        meta_struct: BTreeMap::new(),
        span_links: span
            .links
            .into_iter()
            .map(|link| dd_proto::SpanLink {
                trace_id: u128::from_be_bytes(link.span_context.trace_id().to_bytes()) as u64,
                trace_id_high: 0,
                span_id: u64::from_be_bytes(link.span_context.span_id().to_bytes()),
                attributes: link
                    .attributes
                    .into_iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
                tracestate: link.span_context.trace_state().header(),
                flags: u32::from(link.span_context.trace_flags().to_u8()),
            })
            .collect(),
    }
}

fn trace_into_chunk(
    tags: BTreeMap<String, String>,
    spans: Vec<dd_proto::Span>,
) -> dd_proto::TraceChunk {
    dd_proto::TraceChunk {
        // This should not happen for Datadog originated traces, but in case this field is not populated
        // we default to 1 (https://github.com/DataDog/datadog-agent/blob/eac2327/pkg/trace/sampler/sampler.go#L54-L55),
        // which is what the Datadog trace-agent is doing for OTLP originated traces, as per
        // https://github.com/DataDog/datadog-agent/blob/3ea2eb4/pkg/trace/api/otlp.go#L309.
        priority: 100i32,
        origin: "cloudflare".to_string(),
        spans,
        tags,
        dropped_trace: false,
    }
}

impl DatadogExporter {
    fn trace_into_tracer(&self, chunks: Vec<dd_proto::TraceChunk>) -> dd_proto::TracerPayload {
        dd_proto::TracerPayload {
            container_id: self.container_id.clone(),
            language_name: "rust".to_string(),
            language_version: String::new(),
            tracer_version: VERSION.to_string(),
            runtime_id: self.runtime_id.clone(),
            chunks,
            tags: self.tags.clone(),
            env: self.env.clone(),
            hostname: self.host_name.clone(),
            app_version: self.app_version.clone(),
        }
    }

    fn trace_build(&self, tracer: Vec<dd_proto::TracerPayload>) -> dd_proto::AgentPayload {
        let tags = self.tags.clone();

        dd_proto::AgentPayload {
            host_name: self.host_name.clone(),
            env: self.env.clone(),
            tracer_payloads: tracer,
            tags,
            agent_version: VERSION.to_string(),
            target_tps: 1000f64,
            error_tps: 1000f64,
            rare_sampler_enabled: false,
        }
    }
}

impl DatadogExporter {
    /// Export logs to datadog
    // https://docs.datadoghq.com/api/latest/logs/
    pub fn export_logs(
        &self,
        batch: Vec<LogData>,
    ) -> impl Future<Output = Result<(), DatadogExportError>> + Send + '_ {
        #[derive(Debug, serde::Serialize)]
        pub struct DatadogLogEntry {
            pub ddsource: String,
            pub ddtags: String,
            pub hostname: String,
            pub message: String,
            pub service: String,
            pub status: String,
        }

        let dd_logs = batch
            .into_iter()
            .filter_map(|otel_log| {
                let mut tags = self.tags.clone();
                tags.extend(
                    otel_log
                        .record
                        .attributes
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(key, value)| (key.to_string(), any_to_string(value)))
                        .collect_vec(),
                );

                tags.extend(
                    otel_log
                        .resource
                        .into_iter()
                        .map(|(key, value)| (key.to_string(), value.to_string())),
                );

                otel_log.record.body.map(|log_message| DatadogLogEntry {
                    ddsource: "cloudflare".to_string(),
                    ddtags: tags
                        .iter()
                        .map(|(lhs, rhs)| format!("{lhs}:{rhs}"))
                        .join(","),
                    hostname: self.host_name.to_string(),
                    message: any_to_string(log_message),
                    service: self.service_name.to_string(),
                    status: otel_log
                        .record
                        .severity_number
                        .unwrap_or(Severity::Debug)
                        .name()
                        .to_string()
                        .to_ascii_lowercase(),
                })
            })
            .collect_vec();

        let payload = serde_json::to_string(&dd_logs).unwrap();
        drop(dd_logs);

        SendWrapper::new(async move {
            let logs_request = self
                .client
                .post(self.logs_request_url.to_string())
                .header(http::header::CONTENT_TYPE.to_string(), LOGS_DD_CONTENT_TYPE)
                .header(DEFAULT_DD_API_KEY_HEADER, self.key.clone())
                .body(payload);

            send_request(logs_request).await?;

            Ok(())
        })
    }

    /// Export spans to datadog
    pub fn export_traces(
        &self,
        batch: Vec<SpanData>,
    ) -> impl Future<Output = Result<(), DatadogExportError>> + Send + '_ {
        let traces: Vec<Vec<SpanData>> = group_into_traces(batch, self.sampling);

        let mut chunks: Vec<dd_proto::TraceChunk> = traces
            .into_iter()
            .map(|spans| {
                trace_into_chunk(
                    self.tags.clone(),
                    spans
                        .into_iter()
                        .map(|trace| otel_span_to_dd_span(self, trace))
                        .collect(),
                )
            })
            .collect();

        mark_root_spans(&mut chunks);
        let client_stats = self.compute_client_stats_for_chunks(chunks.iter());

        let traces = self.trace_into_tracer(chunks);

        let stats = dd_proto::StatsPayload {
            agent_hostname: self.host_name.clone(),
            agent_env: self.env.clone(),
            stats: client_stats,
            agent_version: VERSION.to_string(),
            client_computed: true,
            split_payload: false,
        };

        let stats = rmp_serde::to_vec(&stats).map_err(|err| TraceError::from(err.to_string()));

        let trace = self.trace_build(vec![traces]);
        let trace = trace.encode_to_vec();

        SendWrapper::new(async move {
            let trace_request = self
                .client
                .post(self.trace_request_url.to_string())
                .header(
                    http::header::CONTENT_TYPE.to_string(),
                    TRACES_DD_CONTENT_TYPE,
                )
                .header("X-Datadog-Reported-Languages", "rust")
                .header(DEFAULT_DD_API_KEY_HEADER, self.key.clone())
                .body(trace);

            send_request(trace_request).await?;

            if let Ok(stats) = stats {
                let stats_request = self
                    .client
                    .post(self.stats_request_url.to_string())
                    .header(
                        http::header::CONTENT_TYPE.to_string(),
                        STATS_DD_CONTENT_TYPE,
                    )
                    .header("X-Datadog-Reported-Languages", "rust")
                    .header(DEFAULT_DD_API_KEY_HEADER, self.key.clone())
                    .body(stats);

                send_request(stats_request).await?;
            }

            Ok(())
        })
    }

    fn compute_client_stats_for_chunks<'a>(
        &self,
        chunks: impl Iterator<Item = &'a dd_proto::TraceChunk>,
    ) -> Vec<dd_proto::ClientStatsPayload> {
        chunks
            .map(|chunk| {
                let root = chunk.spans.first();
                let leaf = chunk.spans.last();

                let client_group_stats = chunk
                    .spans
                    .iter()
                    .group_by(|span| &span.name)
                    .into_iter()
                    .flat_map(|(span_name, grouped_by_span_name)| {
                        grouped_by_span_name
                            .group_by(|span| {
                                span.meta
                                    .get("http.response.status_code")
                                    .or_else(|| span.meta.get("http.status_code"))
                                    .and_then(|status_code| status_code.parse::<u32>().ok())
                                    .unwrap_or_default()
                            })
                            .into_iter()
                            .map(|(status, spans)| {
                                let grouped_spans = spans.into_iter().collect::<Vec<_>>();
                                let named_span_with_status = grouped_spans.first().unwrap();

                                let hits = grouped_spans.len();
                                let mut errors = 0;
                                let mut top_level_hits = 0;

                                grouped_spans.iter().for_each(|span| {
                                    if span.metrics.get("_top_level").is_some() {
                                        top_level_hits += 1;
                                    }

                                    // todo: improve the error check
                                    // for now I'll just use the group key which is the status from the span metadata
                                    // any span with a status 0 won't be considered for error
                                    if status != 0 && !(200..=299).contains(&status) {
                                        errors += 1;
                                    }
                                });

                                dd_proto::ClientGroupedStats {
                                    service: named_span_with_status.service.clone(),
                                    name: span_name.to_string(),
                                    resource: named_span_with_status.resource.clone(),
                                    http_status_code: status,
                                    r#type: named_span_with_status.r#type.to_string(),
                                    hits: hits as u64,
                                    errors: errors as u64,
                                    duration: named_span_with_status.duration as u64,
                                    peer_tags: vec!["_dd.base_service".to_string()],
                                    top_level_hits,
                                    span_kind: named_span_with_status
                                        .meta
                                        .get("span.kind")
                                        .cloned()
                                        .unwrap_or_default(),
                                    ..Default::default()
                                }
                            })
                            .collect_vec()
                    })
                    .collect_vec();

                dd_proto::ClientStatsPayload {
                    hostname: self.host_name.clone(),
                    env: self.env.clone(),
                    version: self.app_version.clone(),
                    stats: vec![dd_proto::ClientStatsBucket {
                        start: root.map(|root| root.start).unwrap_or_default() as u64,
                        duration: leaf.map(|leaf| leaf.duration).unwrap_or_default() as u64,
                        stats: client_group_stats,
                        ..Default::default()
                    }],
                    lang: "rust".to_string(),
                    tracer_version: VERSION.to_string(),
                    runtime_id: "cloudflare".to_string(),
                    service: self.service_name.clone(),
                    container_id: "cloudflare".to_string(),
                    agent_aggregation: "counts".to_string(),
                    tags: self
                        .tags
                        .clone()
                        .into_iter()
                        .map(|(key, value)| format!("{key}:{value}"))
                        .collect(),
                    ..Default::default()
                }
            })
            .collect_vec()
    }
}

fn mark_root_spans(trace_chunks: &mut Vec<dd_proto::TraceChunk>) {
    for chunk in trace_chunks {
        let mut service_by_parent_id = HashMap::with_capacity(chunk.spans.len());
        for span in &chunk.spans {
            service_by_parent_id.insert(span.parent_id, span.service.clone());
        }

        chunk
            .spans
            .iter_mut()
            .filter(|span| {
                // no parent
                if span.parent_id == 0 {
                    return true;
                }

                let parent = span.parent_id;
                match service_by_parent_id.get(&parent) {
                    Some(parent_service) => {
                        // parent is not in the same service
                        if *parent_service != span.service {
                            true
                        }
                        // anything else is not a root level span
                        else {
                            false
                        }
                    }
                    // parent is not in the current chunk
                    None => true,
                }
            })
            .for_each(|span| {
                span.metrics.insert("_top_level".to_string(), 1.0);
                span.metrics.insert("_dd.top_level".to_string(), 1.0);
                span.metrics.insert("_dd.agent_psr".to_string(), 1.0);
                span.metrics
                    .insert("_sampling_priority_v1".to_string(), 1.0);
                span.metrics.insert("_dd.measured".to_string(), 1.0);

                span.meta
                    .insert("_dd.base_service".to_string(), span.service.clone());
                span.meta
                    .insert("_dd.compute_stats".to_string(), "1".to_string());
                span.meta
                    .insert("_dd.origin".to_string(), "cloudflare".to_string());
                span.meta.insert("_dd.p.dm".to_string(), "-0".to_string());
            })
    }
}

async fn send_request(request: RequestBuilder) -> Result<(), DatadogExportError> {
    let response = request.send().await?;
    let response_status = response.status();

    if !response.status().is_success() {
        return match response.text().await {
            Ok(text) => Err(DatadogExportError::Http(response_status, text)),
            Err(e) => Err(DatadogExportError::Http(response_status, e.to_string())),
        };
    }

    Ok(())
}

pub fn any_to_string(any_value: AnyValue) -> String {
    match any_value {
        AnyValue::Int(i) => i.to_string(),
        AnyValue::Double(d) => d.to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Bytes(b) => base64::engine::general_purpose::STANDARD_NO_PAD.encode(b),
        AnyValue::ListAny(any_list) => any_list.into_iter().map(any_to_string).join(";"),
        AnyValue::Map(any_map) => any_map
            .into_iter()
            .map(|(key, value)| format!("{}:{}", key, any_to_string(value)))
            .join(";"),
    }
}

#[cfg(test)]
mod test {
    use crate::exporter::group_into_traces;
    use opentelemetry::trace::{SpanContext, SpanId, SpanKind};
    use opentelemetry_sdk::export::trace::SpanData;
    use std::time::SystemTime;

    #[test]
    fn test_sampling() {
        let span = SpanData {
            span_context: SpanContext::empty_context(),
            parent_span_id: SpanId::INVALID,
            span_kind: SpanKind::Client,
            name: Default::default(),
            start_time: SystemTime::now(),
            end_time: SystemTime::now(),
            attributes: vec![],
            dropped_attributes_count: 0,
            events: Default::default(),
            links: Default::default(),
            status: Default::default(),
            resource: Default::default(),
            instrumentation_lib: Default::default(),
        };

        let traces = group_into_traces(vec![span.clone()], 0);
        assert!(traces.is_empty());

        let traces = group_into_traces(vec![span], 100);
        assert_eq!(traces.len(), 1);
    }
}
