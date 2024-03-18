use opentelemetry_sdk::export::ExportError;
use rmp::encode::ValueWriteError;

use crate::dd_proto::{ClientGroupedStats, ClientStatsBucket, ClientStatsPayload, StatsPayload};

/// Wrap type for errors from opentelemetry datadog exporter
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Message pack error
    #[error(transparent)]
    MessagePackError(#[from] ValueWriteError),
    /// Message pack error
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// Message pack error
    /// No http client founded. User should provide one or enable features
    #[error("http client must be set, users can enable reqwest or reqwest feature to use http client implementation within create")]
    NoHttpClient,
    /// Http requests failed with following errors
    #[error(transparent)]
    RequestError(#[from] http::Error),
    /// The Uri was invalid
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
    /// Other errors
    #[error("{0}")]
    Other(String),
}

impl ExportError for Error {
    fn exporter_name(&self) -> &'static str {
        "datadog-traces"
    }
}

pub(crate) fn encode_stats(stats_payload: StatsPayload) -> Result<Vec<u8>, Error> {
    let mut encoded = Vec::new();

    rmp::encode::write_str(&mut encoded, "agent_hostname")?;
    rmp::encode::write_str(&mut encoded, stats_payload.agent_hostname.as_str())?;

    rmp::encode::write_str(&mut encoded, "agent_env")?;
    rmp::encode::write_str(&mut encoded, stats_payload.agent_env.as_str())?;

    rmp::encode::write_array_len(&mut encoded, stats_payload.stats.len() as u32)?;
    for stat in stats_payload.stats {
        write_encoded_client_stats(&mut encoded, stat)?;
    }

    rmp::encode::write_str(&mut encoded, "agent_version")?;
    rmp::encode::write_str(&mut encoded, stats_payload.agent_version.as_str())?;

    rmp::encode::write_str(&mut encoded, "client_computed")?;
    rmp::encode::write_bool(&mut encoded, stats_payload.client_computed)?;

    rmp::encode::write_str(&mut encoded, "split_payload")?;
    rmp::encode::write_bool(&mut encoded, stats_payload.split_payload)?;

    Ok(encoded)
}

fn write_encoded_client_stats(
    encoded: &mut Vec<u8>,
    stat: ClientStatsPayload,
) -> Result<(), Error> {
    rmp::encode::write_str(encoded, "hostname")?;
    rmp::encode::write_str(encoded, stat.hostname.as_str())?;

    rmp::encode::write_str(encoded, "env")?;
    rmp::encode::write_str(encoded, stat.env.as_str())?;

    rmp::encode::write_str(encoded, "version")?;
    rmp::encode::write_str(encoded, stat.version.as_str())?;

    rmp::encode::write_array_len(encoded, stat.stats.len() as u32)?;
    for client_stats_bucket in stat.stats {
        write_encoded_client_stats_bucket(encoded, client_stats_bucket)?;
    }

    rmp::encode::write_str(encoded, "lang")?;
    rmp::encode::write_str(encoded, stat.lang.as_str())?;

    rmp::encode::write_str(encoded, "tracer_version")?;
    rmp::encode::write_str(encoded, stat.tracer_version.as_str())?;

    rmp::encode::write_str(encoded, "runtime_id")?;
    rmp::encode::write_str(encoded, stat.runtime_id.as_str())?;

    rmp::encode::write_str(encoded, "sequence")?;
    rmp::encode::write_u64(encoded, stat.sequence)?;

    rmp::encode::write_str(encoded, "agent_aggregation")?;
    rmp::encode::write_str(encoded, stat.agent_aggregation.as_str())?;

    rmp::encode::write_str(encoded, "service")?;
    rmp::encode::write_str(encoded, stat.service.as_str())?;

    rmp::encode::write_str(encoded, "container_id")?;
    rmp::encode::write_str(encoded, stat.container_id.as_str())?;

    rmp::encode::write_array_len(encoded, stat.tags.len() as u32)?;
    for tag in stat.tags {
        rmp::encode::write_str(encoded, tag.as_str())?;
    }

    rmp::encode::write_str(encoded, "git_commit_sha")?;
    rmp::encode::write_str(encoded, stat.git_commit_sha.as_str())?;

    rmp::encode::write_str(encoded, "image_tag")?;
    rmp::encode::write_str(encoded, stat.image_tag.as_str())?;

    Ok(())
}

fn write_encoded_client_stats_bucket(
    encoded: &mut Vec<u8>,
    client_stats_bucket: ClientStatsBucket,
) -> Result<(), Error> {
    rmp::encode::write_str(encoded, "start")?;
    rmp::encode::write_u64(encoded, client_stats_bucket.start)?;

    rmp::encode::write_str(encoded, "duration")?;
    rmp::encode::write_u64(encoded, client_stats_bucket.duration)?;

    rmp::encode::write_array_len(encoded, client_stats_bucket.stats.len() as u32)?;
    for client_grouped_stats in client_stats_bucket.stats {
        write_encoded_client_grouped_stats(encoded, client_grouped_stats)?;
    }

    rmp::encode::write_str(encoded, "agent_time_shift")?;
    rmp::encode::write_i64(encoded, client_stats_bucket.agent_time_shift)?;

    Ok(())
}

fn write_encoded_client_grouped_stats(
    encoded: &mut Vec<u8>,
    client_grouped_stats: ClientGroupedStats,
) -> Result<(), Error> {
    rmp::encode::write_str(encoded, "service")?;
    rmp::encode::write_str(encoded, client_grouped_stats.service.as_str())?;

    rmp::encode::write_str(encoded, "name")?;
    rmp::encode::write_str(encoded, client_grouped_stats.name.as_str())?;

    rmp::encode::write_str(encoded, "resource")?;
    rmp::encode::write_str(encoded, client_grouped_stats.resource.as_str())?;

    rmp::encode::write_str(encoded, "http_status_code")?;
    rmp::encode::write_u32(encoded, client_grouped_stats.http_status_code)?;

    rmp::encode::write_str(encoded, "type")?;
    rmp::encode::write_str(encoded, client_grouped_stats.r#type.as_str())?;

    rmp::encode::write_str(encoded, "db_type")?;
    rmp::encode::write_str(encoded, client_grouped_stats.db_type.as_str())?;

    rmp::encode::write_str(encoded, "hits")?;
    rmp::encode::write_u64(encoded, client_grouped_stats.hits)?;

    rmp::encode::write_str(encoded, "errors")?;
    rmp::encode::write_u64(encoded, client_grouped_stats.errors)?;

    rmp::encode::write_str(encoded, "duration")?;
    rmp::encode::write_u64(encoded, client_grouped_stats.duration)?;

    rmp::encode::write_str(encoded, "ok_summary")?;
    rmp::encode::write_bin_len(encoded, client_grouped_stats.ok_summary.len() as u32)?;
    rmp::encode::write_bin(encoded, client_grouped_stats.ok_summary.as_slice())?;

    rmp::encode::write_str(encoded, "error_summary")?;
    rmp::encode::write_bin_len(encoded, client_grouped_stats.error_summary.len() as u32)?;
    rmp::encode::write_bin(encoded, client_grouped_stats.error_summary.as_slice())?;

    rmp::encode::write_str(encoded, "synthetics")?;
    rmp::encode::write_bool(encoded, client_grouped_stats.synthetics)?;

    rmp::encode::write_str(encoded, "top_level_hits")?;
    rmp::encode::write_u64(encoded, client_grouped_stats.top_level_hits)?;

    rmp::encode::write_str(encoded, "span_kind")?;
    rmp::encode::write_str(encoded, client_grouped_stats.span_kind.as_str())?;

    rmp::encode::write_array_len(encoded, client_grouped_stats.peer_tags.len() as u32)?;
    for tag in client_grouped_stats.peer_tags {
        rmp::encode::write_str(encoded, tag.as_str())?;
    }

    Ok(())
}
