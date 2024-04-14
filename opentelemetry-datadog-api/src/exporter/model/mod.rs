use opentelemetry_sdk::export::ExportError;
use rmp::encode::ValueWriteError;

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
