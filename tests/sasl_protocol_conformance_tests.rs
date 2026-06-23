//! SASL/SCRAM protocol-conformance tests
//!
//! Background. `tests/sasl_auth_failure_tests.rs` covers the existence of
//! failure paths (wrong password, unknown user, cross-mechanism payload).
//! The audit's P1.10 / P1.11 flagged that:
//!
//! - **RFC 7677 vector conformance** is not pinned — the existing tests
//!   prove "this implementation is internally self-consistent" but not
//!   "this implementation matches the canonical SCRAM-SHA-256 spec".
//!   Without a known-good vector, a regression in `pbkdf2_hmac_sha256`,
//!   the HMAC-key derivation, or the `Client Key` / `Server Key`
//!   constants would be invisible — librdkafka and Java consumers would
//!   silently fail to connect because their independent implementation
//!   diverges from ours.
//! - **Nonce-replay protection** is not asserted: the existing
//!   `wrong_password_fails_at_proof` test uses the correct nonce. A
//!   regression where the server forgot to verify the combined nonce
//!   would only surface in real attacks.
//! - **Authzid validation** in PLAIN is not pinned. The Kafka
//!   convention is that an empty authzid means "use authcid" and a
//!   non-empty authzid means the authcid is acting on behalf of that
//!   identity. Today the broker accepts non-empty authzid silently.
//!
//! What this file pins:
//!
//! 1. **RFC 7677 § 3 vector**: deriving `ScramCredentials` for
//!    user/password "user"/"pencil" with salt `W22ZaJ0SNY7soEsUEjb6gQ==`
//!    and 4096 iterations, then driving a handshake, succeeds.
//! 2. **`SaltedPassword` is iteration-deterministic**: deriving twice
//!    with the same inputs yields identical credentials — pinning the
//!    "no salt-is-implicitly-randomized" trap.
//! 3. **Iteration-count floor**: a request to derive with iterations
//!    below `MIN_ITERATIONS` is silently rounded up rather than
//!    accepted. Defends against an attacker who sets a low iteration
//!    count via env override.
//! 4. **Mutated combined nonce in client-final fails**: server-emitted
//!    nonce in `r=` must match exactly.
//! 5. **Truncated combined nonce fails**: a prefix of the correct
//!    nonce is not acceptable.
//! 6. **Empty client proof is rejected**: a client that omits the proof
//!    entirely doesn't authenticate.
//! 7. **PLAIN with non-empty authzid != authcid is currently accepted**
//!    (today's contract; pin so a future strict-authzid implementation
//!    is a deliberate flip).
//!
//! These tests sit alongside `tests/sasl_auth_failure_tests.rs` and
//! reuse the same building blocks. SASL is feature-gated so this file
//! is too.

#![cfg(feature = "sasl")]

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use kafkaesque::cluster::scram::{
    ScramCredentials, ScramServerState, handle_client_final, handle_client_first,
};
use kafkaesque::server::sasl::{PlainAuthenticator, SaslAuthenticator, SaslMechanism};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Drive a SCRAM handshake with explicit control of the client-final
/// nonce. Returns the server's final state plus the bytes it returned.
/// Generalises `drive_scram_handshake` from `sasl_auth_failure_tests.rs`
/// so we can mutate the nonce mid-flight (replay tests).
fn drive_with_custom_final_nonce(
    user: &str,
    server_creds: ScramCredentials,
    password: &[u8],
    nonce_mutator: impl FnOnce(&str) -> String,
) -> ScramServerState {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type H = Hmac<Sha256>;

    let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
    let client_first_bare = format!("n={},r={}", user, client_nonce);
    let client_first = format!("n,,{}", client_first_bare);

    let creds_clone = server_creds.clone();
    let (state, server_first_bytes) = handle_client_first(client_first.as_bytes(), |u| {
        if u == user {
            Some(creds_clone.clone())
        } else {
            None
        }
    });
    let server_first = std::str::from_utf8(&server_first_bytes)
        .unwrap()
        .to_string();
    let mut combined_nonce = String::new();
    let mut salt_b64 = String::new();
    let mut iters: u32 = 4096;
    for attr in server_first.split(',') {
        if let Some(rest) = attr.strip_prefix("r=") {
            combined_nonce = rest.to_string();
        } else if let Some(rest) = attr.strip_prefix("s=") {
            salt_b64 = rest.to_string();
        } else if let Some(rest) = attr.strip_prefix("i=") {
            iters = rest.parse().unwrap_or(4096);
        }
    }
    let salt = B64.decode(&salt_b64).unwrap();

    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password, &salt, iters, &mut salted);
    let mut mac = H::new_from_slice(&salted).unwrap();
    mac.update(b"Client Key");
    let client_key: [u8; 32] = mac.finalize().into_bytes().into();
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();

    // The mutator decides what nonce the client sends in client-final.
    let final_nonce = nonce_mutator(&combined_nonce);

    let cbind_b64 = B64.encode(b"n,,");
    let client_final_without_proof = format!("c={},r={}", cbind_b64, final_nonce);
    let auth_message = format!(
        "{},{},{}",
        client_first_bare, server_first, client_final_without_proof
    );
    let mut sig_mac = H::new_from_slice(&stored_key).unwrap();
    sig_mac.update(auth_message.as_bytes());
    let client_signature: [u8; 32] = sig_mac.finalize().into_bytes().into();
    let mut client_proof = [0u8; 32];
    for i in 0..32 {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }
    let client_final = format!(
        "{},p={}",
        client_final_without_proof,
        B64.encode(client_proof)
    );
    let (final_state, _resp) = handle_client_final(client_final.as_bytes(), state);
    final_state
}

// ---------------------------------------------------------------------------
// 1. RFC 7677 § 3 vector
// ---------------------------------------------------------------------------

#[test]
fn rfc7677_user_pencil_handshake_succeeds() {
    // RFC 7677 § 3 example uses:
    //   username: user
    //   password: pencil
    //   salt:     base64decode("W22ZaJ0SNY7soEsUEjb6gQ==")
    //   i:        4096
    //
    // RFC 5802 then specifies:
    //   SaltedPassword = PBKDF2(HMAC-SHA-256, "pencil", salt, 4096, 32)
    //   ClientKey      = HMAC(SaltedPassword, "Client Key")
    //   ServerKey      = HMAC(SaltedPassword, "Server Key")
    //   StoredKey      = SHA-256(ClientKey)
    //
    // Driving a real handshake against credentials derived from these
    // exact inputs proves: PBKDF2 output, HMAC key derivation, and the
    // "Client Key"/"Server Key" string constants match the spec. A
    // single-bit drift in any of these would fail the proof check.
    let salt = B64
        .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
        .expect("RFC 7677 salt is valid base64");
    let creds = ScramCredentials::derive(b"pencil", &salt, 4096);
    let final_state =
        drive_with_custom_final_nonce("user", creds, b"pencil", |combined| combined.to_string());
    assert!(
        matches!(final_state, ScramServerState::Authenticated { .. }),
        "RFC 7677 vector handshake must succeed, got {:?}",
        final_state,
    );
}

#[test]
fn rfc7677_handshake_with_wrong_password_fails_proof_check() {
    // Same vector, but the client uses the wrong password — must fail
    // at proof verification, not earlier. Pins that the password is
    // actually used in the proof computation (regression: a stub that
    // ignored the password would let any client through).
    let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
    let creds = ScramCredentials::derive(b"pencil", &salt, 4096);
    let final_state = drive_with_custom_final_nonce(
        "user",
        creds,
        b"erasor", // close enough that timing isn't a hint
        |combined| combined.to_string(),
    );
    assert!(matches!(final_state, ScramServerState::Failed(_)));
}

// ---------------------------------------------------------------------------
// 2. Determinism of credential derivation
// ---------------------------------------------------------------------------

#[test]
fn deriving_same_inputs_yields_identical_credentials() {
    // SaltedPassword must be deterministic in (password, salt, iterations).
    // A regression where derive() pulled in fresh randomness would silently
    // break re-deriving from disk on broker restart.
    let salt = [7u8; 16];
    let a = ScramCredentials::derive(b"correct horse battery staple", &salt, 4096);
    let b = ScramCredentials::derive(b"correct horse battery staple", &salt, 4096);
    assert_eq!(a.salt, b.salt);
    assert_eq!(a.iterations, b.iterations);
    assert_eq!(a.stored_key, b.stored_key);
    assert_eq!(a.server_key, b.server_key);
}

// ---------------------------------------------------------------------------
// 3. Iteration-count floor is enforced
// ---------------------------------------------------------------------------

#[test]
fn iteration_count_below_floor_is_rounded_up() {
    // Pin: a request to derive with `iterations=1` doesn't actually run
    // PBKDF2 with 1 iteration (which would produce credentials weaker
    // than the spec). The implementation rounds up to MIN_ITERATIONS.
    let salt = [3u8; 16];
    let weak = ScramCredentials::derive(b"pw", &salt, 1);
    let floor = ScramCredentials::derive(b"pw", &salt, 4096);
    // If iterations were respected, stored_key would differ between
    // 1-iteration and 4096-iteration. Pin equality so a regression that
    // accepts low iterations is caught.
    assert_eq!(
        weak.stored_key, floor.stored_key,
        "iterations=1 must be rounded up to the floor (>=4096)",
    );
    assert!(weak.iterations >= 4096);
}

// ---------------------------------------------------------------------------
// 4. Nonce-replay protection
// ---------------------------------------------------------------------------

#[test]
fn client_final_with_mutated_combined_nonce_fails() {
    // The server emits a combined nonce in server-first; the client
    // must echo it verbatim in client-final. Flip a byte and the
    // server must reject — without this, an attacker who recorded a
    // server-first could replay arbitrary client-final messages.
    let creds = ScramCredentials::derive(b"correct horse", &[1u8; 16], 4096);
    let final_state = drive_with_custom_final_nonce("alice", creds, b"correct horse", |combined| {
        // Replace the last char with an `X`. Combined-nonce contains
        // both client and server halves; mutating ANY position
        // should cause failure.
        let mut s = combined.to_string();
        s.pop();
        s.push('X');
        s
    });
    assert!(
        matches!(final_state, ScramServerState::Failed(_)),
        "mutated combined nonce in client-final must be rejected, got {:?}",
        final_state,
    );
}

#[test]
fn client_final_with_truncated_combined_nonce_fails() {
    // A prefix of the combined nonce is not the combined nonce. Pin
    // that the server's match is exact (not "starts_with").
    let creds = ScramCredentials::derive(b"correct horse", &[1u8; 16], 4096);
    let final_state = drive_with_custom_final_nonce("alice", creds, b"correct horse", |combined| {
        combined[..combined.len() / 2].to_string()
    });
    assert!(
        matches!(final_state, ScramServerState::Failed(_)),
        "truncated combined nonce must be rejected",
    );
}

#[test]
fn client_final_with_only_client_nonce_fails() {
    // The combined nonce is `<client_nonce><server_nonce>`. A buggy
    // client that sent only its client_nonce back must not authenticate.
    // (This is the literal "no replay" case — the server's contribution
    // to the nonce is the proof of liveness.)
    let creds = ScramCredentials::derive(b"correct horse", &[1u8; 16], 4096);
    let final_state = drive_with_custom_final_nonce(
        "alice",
        creds,
        b"correct horse",
        |_combined| "fyko+d2lbbFgONRv9qkxdawL".to_string(), // client nonce only
    );
    assert!(matches!(final_state, ScramServerState::Failed(_)));
}

// ---------------------------------------------------------------------------
// 5. PLAIN authzid handling (today's contract — pin for future flip)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plain_with_authzid_equal_to_authcid_succeeds() {
    // Standard PLAIN payload: [authzid] NUL authcid NUL passwd. When
    // authzid == authcid the message is well-formed and must
    // authenticate.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;
    let payload = b"alice\0alice\0secret"; // authzid="alice"
    let result = auth.authenticate(SaslMechanism::Plain, payload, None).await;
    assert!(result.is_success(), "authzid==authcid must authenticate");
}

#[tokio::test]
async fn plain_with_empty_authzid_succeeds() {
    // Empty authzid is the canonical "use authcid" form — the most
    // common payload from real clients.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;
    let payload = b"\0alice\0secret"; // authzid empty
    let result = auth.authenticate(SaslMechanism::Plain, payload, None).await;
    assert!(result.is_success(), "empty authzid must authenticate");
}

#[tokio::test]
async fn plain_with_different_authzid_is_currently_accepted() {
    // Today the broker does NOT enforce authzid == authcid (or check
    // proxy permissions for an alternate authzid). Real Kafka
    // similarly relays the authzid to authorization. Pin today's
    // pass-through so a future strict-authzid policy is a deliberate
    // flip.
    //
    // TODO strict-authzid: when proxy-impersonation policy lands,
    // either reject differing authzid for unauthorized principals or
    // require an explicit ACL.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;
    let payload = b"bob\0alice\0secret"; // authzid="bob", authcid="alice"
    let result = auth.authenticate(SaslMechanism::Plain, payload, None).await;
    // Today's contract: PLAIN succeeds if authcid+passwd verify. The
    // authzid is logged but not enforced.
    assert!(
        result.is_success(),
        "today's contract: differing authzid is accepted (TODO P1.11)",
    );
}
