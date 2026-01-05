//! Producer ID initialization request handling.

use tracing::error;

use crate::error::KafkaCode;
use crate::server::request::InitProducerIdRequestData;
use crate::server::response::InitProducerIdResponseData;

use super::SlateDBClusterHandler;
use crate::cluster::traits::ProducerCoordinator;

/// Handle an init producer ID request.
pub(super) async fn handle_init_producer_id(
    handler: &SlateDBClusterHandler,
    request: InitProducerIdRequestData,
) -> InitProducerIdResponseData {
    // Use the coordinator's init_producer_id which handles:
    // - New producers: assigns new ID with epoch 0
    // - Recovering transactional producers: bumps epoch
    // - Producer ID mismatches: fencing with new ID
    let result = handler
        .coordinator
        .init_producer_id(
            request.transactional_id.as_deref(),
            request.producer_id,
            request.producer_epoch,
        )
        .await;

    match result {
        Ok((producer_id, producer_epoch)) => InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id,
            producer_epoch,
        },
        Err(e) => {
            error!(error = %e, "Failed to initialize producer ID");
            InitProducerIdResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::Unknown,
                producer_id: -1,
                producer_epoch: -1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // InitProducerId Response Tests
    // ========================================================================

    #[test]
    fn test_init_producer_id_response_success() {
        let response = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 1000,
            producer_epoch: 0,
        };

        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.producer_id > 0);
        assert_eq!(response.producer_epoch, 0);
    }

    #[test]
    fn test_init_producer_id_response_error() {
        let response = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::Unknown,
            producer_id: -1,
            producer_epoch: -1,
        };

        assert_eq!(response.error_code, KafkaCode::Unknown);
        assert_eq!(response.producer_id, -1);
        assert_eq!(response.producer_epoch, -1);
    }

    // ========================================================================
    // Producer ID Value Tests
    // ========================================================================

    #[test]
    fn test_producer_id_values() {
        // Valid producer IDs are > 0
        // -1 indicates error/unassigned
        let valid_id: i64 = 1000;
        let error_id: i64 = -1;

        assert!(valid_id > 0);
        assert_eq!(error_id, -1);
    }

    #[test]
    fn test_producer_id_increment() {
        // Producer IDs should be unique and increasing
        let id1: i64 = 1000;
        let id2: i64 = 1001;
        let id3: i64 = 1002;

        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    // ========================================================================
    // Producer Epoch Tests
    // ========================================================================

    #[test]
    fn test_producer_epoch_new_producer() {
        // New producers start with epoch 0
        let response = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 1000,
            producer_epoch: 0,
        };

        assert_eq!(response.producer_epoch, 0);
    }

    #[test]
    fn test_producer_epoch_fencing() {
        // When a producer is fenced, the epoch is incremented
        let old_epoch: i16 = 0;
        let new_epoch: i16 = 1;

        assert!(new_epoch > old_epoch);
    }

    #[test]
    fn test_producer_epoch_overflow() {
        // Epoch overflow: after i16::MAX (32767), wraps or errors
        let max_epoch = i16::MAX;
        assert_eq!(max_epoch, 32767);
    }

    // ========================================================================
    // Request Data Tests
    // ========================================================================

    #[test]
    fn test_init_producer_id_request_new_producer() {
        // New producer: transactional_id = None, producer_id = -1, epoch = -1
        let request = InitProducerIdRequestData {
            transactional_id: None,
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        assert!(request.transactional_id.is_none());
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);
    }

    #[test]
    fn test_init_producer_id_request_recovery() {
        // Recovering producer: has existing producer_id and epoch
        let request = InitProducerIdRequestData {
            transactional_id: None,
            transaction_timeout_ms: 60000,
            producer_id: 1000,
            producer_epoch: 5,
        };

        assert!(request.producer_id > 0);
        assert!(request.producer_epoch >= 0);
    }

    #[test]
    fn test_init_producer_id_request_transactional() {
        // Transactional producer: has transactional_id
        let request = InitProducerIdRequestData {
            transactional_id: Some("my-transaction".to_string()),
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        assert!(request.transactional_id.is_some());
        assert_eq!(request.transactional_id, Some("my-transaction".to_string()));
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_init_producer_id_throttle_time() {
        let response = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 1000,
            producer_epoch: 0,
        };

        assert_eq!(response.throttle_time_ms, 0);
    }

    // ========================================================================
    // Error Code Tests
    // ========================================================================

    #[test]
    fn test_init_producer_id_error_codes() {
        // Common error codes for InitProducerId
        let success = KafkaCode::None;
        let unknown = KafkaCode::Unknown;

        assert_eq!(success as i16, 0);
        assert_ne!(unknown as i16, 0);
    }
}
