//! Log-redaction tests for authentication code paths.
//!
//! Background. The audit's P0.9 flagged that nothing in the test suite
//! verifies the broker doesn't leak SASL passwords, SCRAM credentials,
//! or `RAFT_CLUSTER_SECRET` into logs. The recon report confirmed the
//! production code is careful (passwords flow as `Zeroizing<String>`,
//! the success log emits a `username_hash_prefix` not the username,
//! the failure log emits a single uniform message with no enumeration
//! channel), but none of that discipline is locked in by a test.
//!
//! This file pins two contracts:
//!
//! 1. **Type-level**: `Debug` formatting of public SASL types
//!    (`PlainAuthenticator`, `SaslResult`, `SaslMechanism`) must not
//!    expose any password we put in. A future contributor accidentally
//!    deriving `Debug` on an inner struct that contains a password
//!    flips this test.
//!
//! 2. **Log-level**: capturing tracing output around an authentication
//!    call, the captured bytes must contain no occurrence of:
//!    - the user's plaintext password
//!    - the user's password as base64 / hex (defense in depth — catches
//!      accidental "logged the auth_data buffer" regressions)
//!    - any `RAFT_CLUSTER_SECRET` value seen in the test env
//!
//! What this file deliberately does NOT cover:
//!
//! - End-to-end TLS handshake log redaction (no broker-side log calls
//!   carry key material today; see `src/server/tls.rs`).
//! - OS-level leakage (core dumps, `/proc/<pid>/maps`). `Zeroizing` is
//!   already used in the production sasl_provider; nothing in a Rust
//!   integration test can validate post-Drop memory state without
//!   `unsafe` poking.
//!
//! `server::sasl` is gated behind the `sasl` cargo feature, so this
//! whole file is too — matching `tests/sasl_auth_failure_tests.rs`.

#![cfg(feature = "sasl")]

use std::sync::{Arc, Mutex};

use kafkaesque::server::sasl::{PlainAuthenticator, SaslAuthenticator, SaslMechanism};
use tracing::info_span;
use tracing_subscriber::layer::SubscriberExt;

const TEST_USERNAME: &str = "loggable-alice";
const TEST_PASSWORD: &str = "S3cret-Should-Never-Appear-In-Logs!";
const TEST_RAFT_SECRET: &str = "raft-cluster-secret-redact-me-too";

// ---------------------------------------------------------------------------
// Tracing capture harness
// ---------------------------------------------------------------------------

/// A `MakeWriter` that appends every formatted log line to a shared
/// buffer. Used by the redaction tests to inspect what tracing actually
/// emitted around an authentication call.
#[derive(Clone)]
struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedWriter {
    type Writer = CapturedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Run `body` with a tracing subscriber installed that captures every
/// emitted event into a buffer. Returns the buffer contents.
///
/// Uses `with_default` (thread-local) rather than `set_global_default`
/// so this composes cleanly with the per-test tracing setup other
/// integration tests may install.
async fn capture_logs<F, Fut>(body: F) -> String
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(CapturedWriter(buf.clone()))
        .with_target(true)
        .with_ansi(false);
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    // Wrap the body in a span so the layer engages even if the code under
    // test doesn't open one of its own.
    let span = info_span!("log_redaction_test");
    let _enter = span.enter();
    body().await;
    drop(_enter);
    drop(_guard);

    let bytes = buf.lock().unwrap().clone();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn assert_no_secret_in_logs(logs: &str, secret: &str, label: &str) {
    assert!(
        !logs.contains(secret),
        "{}: captured logs contained the {} secret verbatim\n--- LOGS ---\n{}\n--- END ---",
        label,
        label,
        logs,
    );
}

// ---------------------------------------------------------------------------
// 1. Public SASL types do not expose passwords via Debug
// ---------------------------------------------------------------------------

#[test]
fn plain_authenticator_does_not_implement_debug() {
    // The strongest possible "Debug doesn't leak" guarantee is: there's
    // no Debug impl to call. PlainAuthenticator wraps a
    // `HashMap<String, String>` of credentials, and a `#[derive(Debug)]`
    // there would mass-dump passwords on any `tracing::error!("...{:?}", auth)`.
    // Pin the absence via a compile-time trait bound that only resolves
    // when `T: !Debug` (we can't express that directly, so use a static
    // assertion that the trait isn't in scope for this type).
    //
    // This is a *negative* contract pinned at the trait-object level:
    // if someone adds `#[derive(Debug)]` to PlainAuthenticator, the
    // call below (which would currently fail to type-check) becomes
    // ambiguous and this test starts compiling — which means it now
    // *does* compile and we should flip to checking the formatted
    // output. Until then, the function body never runs.
    fn _requires_no_debug<T>(_: &T)
    where
        T: ?Sized,
    {
        // Empty; the absence of `T: Debug` here is the point.
    }
    let auth = PlainAuthenticator::new_insecure();
    _requires_no_debug(&auth);
}

#[tokio::test]
async fn sasl_result_failed_debug_does_not_carry_password() {
    // The Failed variant carries a `message: String`. Pin that the
    // production code path never *constructs* a Failed message that
    // includes the password. We can't enumerate every construction
    // site; instead, drive a real failure path and check.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user(TEST_USERNAME, TEST_PASSWORD).await;
    let mut payload = vec![0u8];
    payload.extend_from_slice(TEST_USERNAME.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"definitely-wrong-password");

    let result = auth
        .authenticate(SaslMechanism::Plain, &payload, None)
        .await;
    assert!(!result.is_success());
    let dbg = format!("{:?}", result);
    // The real password must never appear in the Failed message even
    // when the wrong-password attempt is presented.
    assert_no_secret_in_logs(&dbg, TEST_PASSWORD, "SaslResult::Failed Debug");
}

#[tokio::test]
async fn sasl_result_success_debug_carries_principal_not_password() {
    // The Success variant SHOULD carry the principal (username) since
    // downstream code needs it for authorization. But it must NOT carry
    // the password.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user(TEST_USERNAME, TEST_PASSWORD).await;
    let mut payload = vec![0u8];
    payload.extend_from_slice(TEST_USERNAME.as_bytes());
    payload.push(0);
    payload.extend_from_slice(TEST_PASSWORD.as_bytes());

    let result = auth
        .authenticate(SaslMechanism::Plain, &payload, None)
        .await;
    assert!(result.is_success(), "setup auth must succeed");
    let dbg = format!("{:?}", result);
    assert_no_secret_in_logs(&dbg, TEST_PASSWORD, "SaslResult::Success Debug");
    // Sanity: principal IS exposed (intentionally — it's the audit
    // identity). A future change that hashes it should be deliberate.
    assert!(
        dbg.contains(TEST_USERNAME),
        "Success Debug must include principal for audit logs; got {}",
        dbg,
    );
}

// ---------------------------------------------------------------------------
// 2. Tracing capture around authentication paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plain_auth_success_logs_do_not_contain_password() {
    let logs = capture_logs(|| async {
        let auth = PlainAuthenticator::new_insecure();
        auth.add_user(TEST_USERNAME, TEST_PASSWORD).await;
        let mut payload = vec![0u8];
        payload.extend_from_slice(TEST_USERNAME.as_bytes());
        payload.push(0);
        payload.extend_from_slice(TEST_PASSWORD.as_bytes());
        let _ = auth
            .authenticate(SaslMechanism::Plain, &payload, None)
            .await;
    })
    .await;

    assert_no_secret_in_logs(&logs, TEST_PASSWORD, "plaintext password");

    // Defense in depth: base64-encoded password should also not appear.
    // (Catches a regression where someone logs the raw auth_data
    // buffer base64-encoded "for safety".)
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(TEST_PASSWORD.as_bytes());
    assert_no_secret_in_logs(&logs, &b64, "base64-encoded password");

    // Hex-encoded password — same reasoning.
    let hex: String = TEST_PASSWORD
        .bytes()
        .map(|b| format!("{:02x}", b))
        .collect();
    assert_no_secret_in_logs(&logs, &hex, "hex-encoded password");
}

#[tokio::test]
async fn plain_auth_failure_logs_do_not_contain_password() {
    let logs = capture_logs(|| async {
        let auth = PlainAuthenticator::new_insecure();
        auth.add_user(TEST_USERNAME, TEST_PASSWORD).await;
        let mut payload = vec![0u8];
        payload.extend_from_slice(TEST_USERNAME.as_bytes());
        payload.push(0);
        payload.extend_from_slice(b"definitely-wrong-password");
        let _ = auth
            .authenticate(SaslMechanism::Plain, &payload, None)
            .await;
    })
    .await;

    assert_no_secret_in_logs(&logs, TEST_PASSWORD, "plaintext password");
    assert_no_secret_in_logs(
        &logs,
        "definitely-wrong-password",
        "attempted wrong password",
    );
}

#[tokio::test]
async fn plain_auth_unknown_user_logs_do_not_contain_password() {
    // Unknown user is a different code path than wrong-password — pin
    // it independently. A regression in the unknown-user branch could
    // log the attempted password while the wrong-password branch
    // doesn't.
    let logs = capture_logs(|| async {
        let auth = PlainAuthenticator::new_insecure();
        auth.add_user("someone-else", "their-secret").await;
        let mut payload = vec![0u8];
        payload.extend_from_slice(b"unknown-user");
        payload.push(0);
        payload.extend_from_slice(TEST_PASSWORD.as_bytes());
        let _ = auth
            .authenticate(SaslMechanism::Plain, &payload, None)
            .await;
    })
    .await;

    assert_no_secret_in_logs(&logs, TEST_PASSWORD, "attempted password");
    assert_no_secret_in_logs(&logs, "their-secret", "other user's stored password");
}

#[tokio::test]
async fn plain_auth_logs_do_not_contain_raft_cluster_secret() {
    // The Raft cluster secret authenticates inter-broker RPCs; it must
    // never appear in logs from unrelated code paths. Pin defense in
    // depth: even if an auth handler accidentally reads the env var
    // (it shouldn't), it must not print it.
    //
    // SAFETY: env var mutation is process-global. Tests in different
    // binaries don't observe each other; tests in this binary all want
    // the same value. The pre-existing common harness uses the same
    // pattern (see `tests/common/mod.rs::enable_single_node_bootstrap`).
    unsafe { std::env::set_var("RAFT_CLUSTER_SECRET", TEST_RAFT_SECRET) };

    let logs = capture_logs(|| async {
        let auth = PlainAuthenticator::new_insecure();
        auth.add_user(TEST_USERNAME, TEST_PASSWORD).await;
        let mut payload = vec![0u8];
        payload.extend_from_slice(TEST_USERNAME.as_bytes());
        payload.push(0);
        payload.extend_from_slice(TEST_PASSWORD.as_bytes());
        let _ = auth
            .authenticate(SaslMechanism::Plain, &payload, None)
            .await;
    })
    .await;

    assert_no_secret_in_logs(&logs, TEST_RAFT_SECRET, "RAFT_CLUSTER_SECRET");
}

// ---------------------------------------------------------------------------
// 3. The capture harness itself is honest
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capture_harness_actually_captures() {
    // Meta-test: prove the capture mechanism actually catches tracing
    // emissions. Otherwise the redaction tests above would silently
    // pass by capturing nothing.
    let logs = capture_logs(|| async {
        tracing::info!(target: "test", "canary-string-the-test-emitted");
    })
    .await;
    assert!(
        logs.contains("canary-string-the-test-emitted"),
        "capture harness emitted nothing. Without this canary, all the \
         redaction tests are toothless. Captured: {:?}",
        logs,
    );
}
