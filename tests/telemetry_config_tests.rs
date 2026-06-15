//! Tests for TelemetryConfig and LogFormat.

use kafkaesque::telemetry::{LogFormat, TelemetryConfig};

#[test]
fn test_log_format_copy() {
    let format = LogFormat::Json;
    let copied = format;
    assert_eq!(format, copied);
}

#[test]
fn test_telemetry_config_custom_values() {
    let config = TelemetryConfig {
        service_name: "my-custom-service".to_string(),
        otlp_endpoint: "http://custom:4318".to_string(),
        enable_console: false,
    };

    assert_eq!(config.service_name, "my-custom-service");
    assert_eq!(config.otlp_endpoint, "http://custom:4318");
    assert!(!config.enable_console);
}

#[test]
fn test_telemetry_config_default() {
    let config = TelemetryConfig::default();
    assert!(!config.service_name.is_empty());
}

#[test]
fn test_log_format_variants() {
    let json = LogFormat::Json;
    let pretty = LogFormat::Pretty;

    let json_debug = format!("{:?}", json);
    let pretty_debug = format!("{:?}", pretty);

    assert!(json_debug.contains("Json"));
    assert!(pretty_debug.contains("Pretty"));
}

#[test]
fn test_log_format_clone() {
    let original = LogFormat::Json;
    let cloned = original;
    assert_eq!(original, cloned);
}

#[test]
fn test_log_format_eq() {
    assert_eq!(LogFormat::Json, LogFormat::Json);
    assert_eq!(LogFormat::Pretty, LogFormat::Pretty);
    assert_ne!(LogFormat::Json, LogFormat::Pretty);
}
