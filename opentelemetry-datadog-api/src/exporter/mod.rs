#[cfg(target_arch = "wasm32")]
use getrandom as _;

mod model;

use async_trait::async_trait;
use http::Uri;
use itertools::Itertools;
pub use model::Error;
use opentelemetry::trace::{SpanId, TraceResult};
use opentelemetry::trace::{Status, TraceError};
use opentelemetry::Key;
use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_sdk::export::trace;
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::resource::ResourceDetector;
use opentelemetry_sdk::resource::SdkProvidedResourceDetector;
use opentelemetry_sdk::trace::Config;
use opentelemetry_sdk::trace::Span;
use opentelemetry_sdk::trace::SpanProcessor;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions as semcov;
use prost::Message;
use send_wrapper::SendWrapper;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::dd_proto;

#[cfg(not(feature = "reqwest-client"))]
use reqwest as _;
use reqwest::{Client, RequestBuilder};

const DEFAULT_SITE_ENDPOINT: &str = "https://trace.agent.datadoghq.eu/";
const DEFAULT_DD_TRACES_PATH: &str = "api/v0.2/traces";
const DEFAULT_DD_STATS_PATH: &str = "api/v0.2/stats";
const TRACES_DD_CONTENT_TYPE: &str = "application/x-protobuf";
const STATS_DD_CONTENT_TYPE: &str = "application/msgpack";
const DEFAULT_DD_API_KEY_HEADER: &str = "DD-Api-Key";
const DEFAULT_FLUSH_SIZE: usize = 500;

const VERSION: &str = env!("CARGO_PKG_VERSION");

thread_local! {
    static SPANS: RefCell<Vec<SpanData>> = RefCell::new(Vec::new());
}

/// Datadog span exporter
#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct DatadogExporter {
    client: Arc<Client>,
    trace_request_url: Uri,
    stats_request_url: Uri,
    service_name: String,
    env: String,
    tags: BTreeMap<String, String>,
    host_name: String,
    key: String,
    runtime_id: String,
    container_id: String,
    app_version: String,
    flush_size: usize,
}

impl DatadogExporter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        service_name: String,
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
        flush_size: usize,
    ) -> Self {
        DatadogExporter {
            client,
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
            flush_size,
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
    agent_endpoint: String,
    api_key: Option<String>,
    app_version: Option<String>,
    client: Option<Arc<Client>>,
    container_id: Option<String>,
    env: Option<String>,
    flush_size: Option<usize>,
    host_name: Option<String>,
    runtime_id: Option<String>,
    service_name: Option<String>,
    tags: Option<BTreeMap<String, String>>,
    trace_config: Option<opentelemetry_sdk::trace::Config>,
}

impl Default for DatadogPipelineBuilder {
    fn default() -> Self {
        DatadogPipelineBuilder {
            service_name: None,
            agent_endpoint: DEFAULT_SITE_ENDPOINT.to_string(),
            trace_config: None,
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
            flush_size: None,
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
        let (_, service_name) = self.build_config_and_service_name();
        self.build_exporter_with_service_name(service_name)
    }

    fn build_config_and_service_name(&mut self) -> (Config, String) {
        let service_name = self.service_name.take();
        if let Some(service_name) = service_name {
            let config = if let Some(mut config) = self.trace_config.take() {
                config.resource = {
                    let without_service_name = config
                        .resource
                        .iter()
                        .filter(|(k, _v)| {
                            **k != Key::new(semcov::resource::SERVICE_NAME.to_string())
                        })
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect::<Vec<KeyValue>>();
                    std::borrow::Cow::Owned(Resource::new(without_service_name))
                };
                config
            } else {
                Config {
                    resource: std::borrow::Cow::Owned(Resource::empty()),
                    ..Default::default()
                }
            };
            (config, service_name)
        } else {
            let service_name = SdkProvidedResourceDetector
                .detect(Duration::from_secs(0))
                .get(Key::new(semcov::resource::SERVICE_NAME.to_string()))
                .unwrap()
                .to_string();
            (
                Config {
                    // use a empty resource to prevent TracerProvider to assign a service name.
                    resource: std::borrow::Cow::Owned(Resource::empty()),
                    ..Default::default()
                },
                service_name,
            )
        }
    }

    fn build_exporter_with_service_name(
        self,
        service_name: String,
    ) -> Result<DatadogExporter, TraceError> {
        if let Some(client) = self.client {
            let traces_endpoint = self.agent_endpoint.clone() + DEFAULT_DD_TRACES_PATH;
            let stats_endpoint = self.agent_endpoint + DEFAULT_DD_STATS_PATH;
            let exporter = DatadogExporter::new(
                service_name,
                stats_endpoint.parse().map_err::<Error, _>(Into::into)?,
                traces_endpoint.parse().map_err::<Error, _>(Into::into)?,
                client,
                self.api_key
                    .ok_or_else(|| TraceError::Other("APIKey not provied".into()))?,
                self.env.unwrap_or_default(),
                self.tags.unwrap_or_default(),
                self.host_name.unwrap_or_default(),
                self.runtime_id.unwrap_or_default(),
                self.container_id.unwrap_or_default(),
                self.app_version.unwrap_or_default(),
                self.flush_size.unwrap_or(DEFAULT_FLUSH_SIZE),
            );
            Ok(exporter)
        } else {
            Err(Error::NoHttpClient.into())
        }
    }

    /// Install the Datadog worker trace exporter pipeline using a simple span processor.
    ///
    /// # Errors
    ///
    /// If the Endpoint or the `APIKey` are not properly set.
    /// Install the Datadog worker trace exporter pipeline using a simple span processor.
    ///
    /// # Errors
    ///
    /// If the Endpoint or the `APIKey` are not properly set.
    pub fn install(
        mut self,
    ) -> Result<
        (
            opentelemetry_sdk::trace::Tracer,
            opentelemetry_sdk::trace::TracerProvider,
        ),
        TraceError,
    > {
        let (config, service_name) = self.build_config_and_service_name();
        let exporter = self.build_exporter_with_service_name(service_name.clone())?;
        let flush_size = exporter.flush_size;
        let span_processor = WASMWorkerSpanProcessor::new(exporter, flush_size);
        let mut provider_builder =
            opentelemetry_sdk::trace::TracerProvider::builder().with_span_processor(span_processor);
        provider_builder = provider_builder.with_config(config);
        let provider = provider_builder.build();
        let tracer = provider.versioned_tracer(
            "opentelemetry-datadog-api",
            Some(env!("CARGO_PKG_VERSION")),
            None::<String>,
            Some(vec![KeyValue::new("service", service_name)]),
        );

        Ok((tracer, provider))
    }

    /// Assign the service name under which to group traces
    #[must_use]
    pub fn with_service_name<T: Into<String>>(mut self, name: T) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Assign the Datadog trace endpoint
    #[must_use]
    pub fn with_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.agent_endpoint = endpoint.into();
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

    /// Assign the SDK trace configuration
    #[must_use]
    pub fn with_trace_config(mut self, config: opentelemetry_sdk::trace::Config) -> Self {
        self.trace_config = Some(config);
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

    /// Assign the tags
    #[must_use]
    pub fn with_flush_size(mut self, flush_size: usize) -> Self {
        self.flush_size = Some(flush_size);
        self
    }
}

fn group_into_traces(spans: Vec<SpanData>) -> Vec<Vec<SpanData>> {
    spans
        .into_iter()
        .into_group_map_by(|span_data| span_data.span_context.trace_id())
        .into_values()
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
        origin: "lambda".to_string(),
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
    /// Export spans to datadog
    // TODO: Should split & batch them when it's too big, check Vector reference.
    pub fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl Future<Output = trace::ExportResult> + Send + '_ {
        let traces: Vec<Vec<SpanData>> = group_into_traces(batch);

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

        let stats = model::encode_stats(stats).map_err(TraceError::from);

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
                            })
                            .into_iter()
                            .flat_map(|(http_status, spans)| {
                                let status: http::StatusCode = http_status
                                    .and_then(|status| status.parse().ok())
                                    .unwrap_or_default();
                                let hits = spans.size_hint().0;
                                let errors = if status.is_success() { 0 } else { hits };

                                spans.into_iter().map(move |named_span_with_status| {
                                    dd_proto::ClientGroupedStats {
                                        service: named_span_with_status.service.clone(),
                                        name: span_name.to_string(),
                                        resource: named_span_with_status.resource.clone(),
                                        http_status_code: status.as_u16() as u32,
                                        r#type: named_span_with_status.r#type.to_string(),
                                        hits: hits as u64,
                                        errors: errors as u64,
                                        duration: named_span_with_status.duration as u64,
                                        peer_tags: vec!["_dd.base_service".to_string()],
                                        ..Default::default()
                                    }
                                })
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
                span.metrics.insert("_dd.agent_psr".to_string(), 1.0);
                span.metrics
                    .insert("_sampling_priority_v1".to_string(), 1.0);
                span.metrics.insert("_dd.measured".to_string(), 1.0);

                span.meta
                    .insert("_dd.base_service".to_string(), span.service.clone());
                span.meta
                    .insert("_dd.compute_stats".to_string(), "1".to_string());
                span.meta
                    .insert("_dd.origin".to_string(), "lambda".to_string());
                span.meta.insert("_dd.p.dm".to_string(), "-0".to_string());
            })
    }
}

async fn send_request(request: RequestBuilder) -> Result<(), TraceError> {
    let response = match request.send().await {
        Ok(response) => response,
        Err(e) => return Err(TraceError::from(e.to_string())),
    };

    if !response.status().is_success() {
        return match response.text().await {
            Ok(text) => Err(TraceError::from(text)),
            Err(e) => Err(TraceError::from(e.to_string())),
        };
    }

    Ok(())
}
