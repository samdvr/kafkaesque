//! Telemetry and logging configuration for Kafkaesque.
//!
//! This module provides:
//! - Configurable logging with JSON or pretty-print formats
//! - Optional OpenTelemetry integration for distributed tracing
//!
//! # Basic Logging
//!
//! ```rust,no_run
//! use kafkaesque::telemetry::{LogFormat, init_logging};
//!
//! // Initialize pretty logging (default)
//! init_logging(LogFormat::Pretty).expect("Failed to init logging");
//!
//! // Or JSON logging for production
//! init_logging(LogFormat::Json).expect("Failed to init logging");
//! ```
//!
//! # Environment Variables
//!
//! - `LOG_FORMAT`: Set to `json` or `pretty` (default: `pretty`)
//! - `RUST_LOG`: Control log levels (default: `info`)
//!
//! # OpenTelemetry (Optional)
//!
//! Enable the `otel` feature in Cargo.toml:
//!
//! ```toml
//! [dependencies]
//! kafkaesque = { version = "0.1", features = ["otel"] }
//! ```
//!
//! Then initialize telemetry at application startup:
//!
//! ```rust,no_run
//! use kafkaesque::telemetry::{TelemetryConfig, init_telemetry};
//!
//! #[tokio::main]
//! async fn main() {
//!     // Initialize with default OTLP endpoint (localhost:4317)
//!     let config = TelemetryConfig::default();
//!     init_telemetry(config).expect("Failed to init telemetry");
//!
//!     // Your application code...
//!
//!     // Shutdown gracefully
//!     kafkaesque::telemetry::shutdown_telemetry();
//! }
//! ```
//!
//! # OpenTelemetry Environment Variables
//!
//! - `OTEL_EXPORTER_OTLP_ENDPOINT`: Override the OTLP endpoint (default: `http://localhost:4317`)
//! - `OTEL_SERVICE_NAME`: Override the service name (default: `kafkaesque-kafka-broker`)
//!
//! # Architecture
//!
//! This module uses `tracing-opentelemetry` to bridge existing `tracing` spans
//! to OpenTelemetry. All existing `tracing::instrument` attributes and manual
//! spans are automatically exported to your OpenTelemetry collector.

use tracing_subscriber::prelude::*;

#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider;
#[cfg(feature = "otel")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "otel")]
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable pretty-print format (default).
    #[default]
    Pretty,
    /// JSON format for log aggregators (Elasticsearch, Loki, etc.).
    Json,
}

impl std::str::FromStr for LogFormat {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Pretty,
        })
    }
}

impl LogFormat {
    /// Read from LOG_FORMAT environment variable.
    pub fn from_env() -> Self {
        std::env::var("LOG_FORMAT")
            .map(|s| s.parse().unwrap_or_default())
            .unwrap_or_default()
    }
}

/// Initialize logging with the specified format.
///
/// This sets up the tracing subscriber with either JSON or pretty-print output.
/// Log levels are controlled via the `RUST_LOG` environment variable.
///
/// Note: JSON logging requires the `json` feature on tracing-subscriber.
/// Without it, JSON format kafkaesques back to pretty format with a warning.
///
/// # Example
///
/// ```rust,no_run
/// use kafkaesque::telemetry::{LogFormat, init_logging};
///
/// // Pretty logging for development
/// init_logging(LogFormat::Pretty).expect("Failed to init logging");
///
/// // JSON logging for production (or use LOG_FORMAT=json)
/// // init_logging(LogFormat::Json).expect("Failed to init logging");
/// ```
pub fn init_logging(format: LogFormat) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    match format {
        LogFormat::Json => {
            // JSON logging - uses pretty format as fallback
            // To enable true JSON, add `features = ["json"]` to tracing-subscriber in Cargo.toml
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .try_init()?;
            tracing::warn!(
                "JSON logging requested but json feature not enabled, using pretty format"
            );
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .try_init()?;
        }
    }

    Ok(())
}

/// Configuration for OpenTelemetry telemetry.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Service name for traces (default: "kafkaesque-kafka-broker")
    pub service_name: String,

    /// OTLP endpoint URL (default: "http://localhost:4317")
    pub otlp_endpoint: String,

    /// Whether to enable console output alongside OTel export
    pub enable_console: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "kafkaesque-kafka-broker".to_string()),
            otlp_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4317".to_string()),
            enable_console: true,
        }
    }
}

/// Initialize OpenTelemetry tracing.
///
/// This sets up the tracing subscriber with OpenTelemetry export.
/// Call this once at application startup before any tracing calls.
///
/// # Errors
///
/// Returns an error if the OTLP exporter cannot be initialized.
///
/// # Example
///
/// ```rust,no_run
/// use kafkaesque::telemetry::{TelemetryConfig, init_telemetry};
///
/// let config = TelemetryConfig {
///     service_name: "my-broker".to_string(),
///     otlp_endpoint: "http://jaeger:4317".to_string(),
///     enable_console: true,
/// };
///
/// init_telemetry(config).expect("Failed to init telemetry");
/// ```
#[cfg(feature = "otel")]
pub fn init_telemetry(config: TelemetryConfig) -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;

    // Create resource with service name
    let resource = Resource::new(vec![KeyValue::new(
        "service.name",
        config.service_name.clone(),
    )]);

    // Create OTLP exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()?;

    // Create tracer provider
    let provider = sdktrace::TracerProvider::builder()
        .with_batch_exporter(exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    // Get tracer from provider
    let tracer = provider.tracer("kafkaesque");

    // Register the provider globally
    opentelemetry::global::set_tracer_provider(provider);

    // Create OpenTelemetry layer
    let otel_layer = OpenTelemetryLayer::new(tracer);

    // Build subscriber with OTel layer
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // Always use fmt layer with OpenTelemetry
    let fmt_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    tracing::info!(
        service_name = %config.service_name,
        otlp_endpoint = %config.otlp_endpoint,
        "OpenTelemetry tracing initialized"
    );

    Ok(())
}

/// Initialize telemetry (no-op when otel feature is disabled).
#[cfg(not(feature = "otel"))]
pub fn init_telemetry(_config: TelemetryConfig) -> Result<(), Box<dyn std::error::Error>> {
    // No-op when otel feature is disabled
    Ok(())
}

/// Shutdown OpenTelemetry gracefully.
///
/// Call this before application exit to ensure all pending traces are flushed.
#[cfg(feature = "otel")]
pub fn shutdown_telemetry() {
    opentelemetry::global::shutdown_tracer_provider();
    tracing::info!("OpenTelemetry tracing shut down");
}

/// Shutdown telemetry (no-op when otel feature is disabled).
#[cfg(not(feature = "otel"))]
pub fn shutdown_telemetry() {
    // No-op when otel feature is disabled
}

/// Check if OpenTelemetry is enabled.
#[cfg(feature = "otel")]
pub fn is_otel_enabled() -> bool {
    true
}

/// Check if OpenTelemetry is enabled (always false when feature disabled).
#[cfg(not(feature = "otel"))]
pub fn is_otel_enabled() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_str() {
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("pretty".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("PRETTY".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("anything".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
    }

    #[test]
    fn test_log_format_default() {
        assert_eq!(LogFormat::default(), LogFormat::Pretty);
    }

    #[test]
    fn test_log_format_from_str_edge_cases() {
        // Test various case combinations
        assert_eq!("Json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("jSoN".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("PRETTY".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("Pretty".parse::<LogFormat>().unwrap(), LogFormat::Pretty);

        // Unknown values default to Pretty
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("xml".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
    }

    #[test]
    fn test_default_config() {
        let config = TelemetryConfig::default();
        assert!(config.service_name.contains("kafkaesque"));
        assert!(config.otlp_endpoint.contains("4317"));
        assert!(config.enable_console);
    }

    #[test]
    fn test_custom_config() {
        let config = TelemetryConfig {
            service_name: "custom-service".to_string(),
            otlp_endpoint: "http://jaeger:4317".to_string(),
            enable_console: false,
        };
        assert_eq!(config.service_name, "custom-service");
        assert_eq!(config.otlp_endpoint, "http://jaeger:4317");
        assert!(!config.enable_console);
    }

    #[test]
    fn test_config_debug() {
        let config = TelemetryConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("TelemetryConfig"));
        assert!(debug_str.contains("service_name"));
        assert!(debug_str.contains("otlp_endpoint"));
    }

    #[test]
    fn test_config_clone() {
        let config = TelemetryConfig {
            service_name: "test".to_string(),
            otlp_endpoint: "http://localhost:4317".to_string(),
            enable_console: true,
        };
        let cloned = config.clone();
        assert_eq!(config.service_name, cloned.service_name);
        assert_eq!(config.otlp_endpoint, cloned.otlp_endpoint);
        assert_eq!(config.enable_console, cloned.enable_console);
    }

    #[test]
    fn test_log_format_debug() {
        let pretty_debug = format!("{:?}", LogFormat::Pretty);
        let json_debug = format!("{:?}", LogFormat::Json);
        assert!(pretty_debug.contains("Pretty"));
        assert!(json_debug.contains("Json"));
    }

    #[test]
    fn test_log_format_eq() {
        assert_eq!(LogFormat::Pretty, LogFormat::Pretty);
        assert_eq!(LogFormat::Json, LogFormat::Json);
        assert_ne!(LogFormat::Pretty, LogFormat::Json);
    }

    #[test]
    fn test_otel_enabled_flag() {
        // This will be true if otel feature is enabled, false otherwise
        let _enabled = is_otel_enabled();
        // Just verify it doesn't panic
    }

    #[test]
    fn test_shutdown_noop() {
        // Shutdown should be safe to call even if init wasn't called
        shutdown_telemetry();
    }

    #[test]
    fn test_init_telemetry_without_otel() {
        // When otel feature is disabled, this should just return Ok
        let config = TelemetryConfig::default();
        let result = init_telemetry(config);
        // Without the otel feature, this is a no-op and should succeed
        assert!(result.is_ok());
    }
}
