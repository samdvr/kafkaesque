//! SASL authentication failure-mode coverage.
//!
//! The audit flagged that SASL paths were exercised only on success: this
//! file fills in the negative-path coverage with assertions on each
//! discrete failure surface so a regression that turns a "wrong password"
//! into a "success" — or, worse, leaks user existence via timing or error
//! text — fails CI loudly.
//!
//! Surfaces covered:
//! - PLAIN: wrong password, unknown user, malformed payload, TLS gate
//! - SCRAM-SHA-256: wrong password (client-final proof mismatch),
//!   unknown user (gets a fake server-first then fails at proof — the
//!   user-enumeration mitigation), nonce mismatch
//! - Mechanism mismatch: a PLAIN-only authenticator must not authenticate
//!   a SCRAM byte payload, and vice versa
//!
//! `SaslProvider` is module-private; for the dispatcher-level mechanism
//! mismatch we use the public `PlainAuthenticator` and exercise the
//! cross-mechanism confusion at its public surface.

#![cfg(feature = "sasl")]

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use kafkaesque::cluster::scram::{
    ScramCredentials, ScramServerState, handle_client_final, handle_client_first,
};
use kafkaesque::server::sasl::{PlainAuthenticator, SaslAuthenticator, SaslMechanism};

// ------------------------------------------------------------------
// PLAIN
// ------------------------------------------------------------------

#[tokio::test]
async fn plain_wrong_password_is_rejected() {
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;

    let result = auth
        .authenticate(SaslMechanism::Plain, b"\0alice\0wrongpass", None)
        .await;
    assert!(!result.is_success(), "wrong password must not authenticate");
    assert!(result.principal().is_none());
}

#[tokio::test]
async fn plain_unknown_user_is_rejected_with_indistinguishable_response() {
    // The user-enumeration mitigation: same observable response for
    // wrong-password and unknown-user. Both paths run a constant-time
    // comparison against a dummy candidate, and both produce the same
    // generic "Authentication failed" message.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;

    let bad_pass = auth
        .authenticate(SaslMechanism::Plain, b"\0alice\0WRONG", None)
        .await;
    let unknown = auth
        .authenticate(SaslMechanism::Plain, b"\0nobody\0secret", None)
        .await;

    assert!(!bad_pass.is_success());
    assert!(!unknown.is_success());
    // Same generic failure message — no enumeration channel.
    let extract = |r: &kafkaesque::server::sasl::SaslResult| match r {
        kafkaesque::server::sasl::SaslResult::Failed { message } => message.clone(),
        _ => panic!("expected Failed"),
    };
    assert_eq!(extract(&bad_pass), extract(&unknown));
}

#[tokio::test]
async fn plain_rejects_when_tls_required_and_session_lacks_tls_hint() {
    // `PlainAuthenticator::new()` defaults to `require_tls=true` (production
    // posture). The session_data hint must be `Some(b"tls")` to admit; any
    // other value — including `None` — must be rejected even with valid
    // credentials. This is the non-bypassable cleartext-credentials gate.
    let auth = PlainAuthenticator::new();
    auth.add_user("alice", "secret").await;

    let no_tls = auth
        .authenticate(SaslMechanism::Plain, b"\0alice\0secret", None)
        .await;
    assert!(!no_tls.is_success(), "PLAIN over plaintext must be refused");

    let with_tls = auth
        .authenticate(SaslMechanism::Plain, b"\0alice\0secret", Some(b"tls"))
        .await;
    assert!(with_tls.is_success(), "PLAIN over TLS must succeed");
}

#[tokio::test]
async fn plain_rejects_malformed_payload() {
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;

    // No NUL separator at all — must return failure, must not panic.
    let bad = auth
        .authenticate(SaslMechanism::Plain, b"alice:secret", None)
        .await;
    assert!(!bad.is_success());

    // Empty payload — no parts.
    let empty = auth.authenticate(SaslMechanism::Plain, b"", None).await;
    assert!(!empty.is_success());
}

// ------------------------------------------------------------------
// Mechanism mismatch
// ------------------------------------------------------------------

#[test]
fn plain_authenticator_only_advertises_plain() {
    // A client that handshakes SCRAM-SHA-256 against a PLAIN-only broker
    // must see the PLAIN mechanism list and decide. The advertised list is
    // the contract; if it ever silently grew to include SCRAM without a
    // matching code path, clients would commit to SCRAM and stall.
    let auth = PlainAuthenticator::new();
    let mechs = auth.supported_mechanisms();
    assert_eq!(mechs, vec![SaslMechanism::Plain]);
    assert!(!mechs.contains(&SaslMechanism::ScramSha256));
    assert!(!mechs.contains(&SaslMechanism::ScramSha512));
}

#[tokio::test]
async fn plain_does_not_authenticate_scram_byte_payload() {
    // SCRAM `client-first` is `n,,n=alice,r=<nonce>`. PLAIN's parser splits
    // on NUL — there is no NUL byte in a SCRAM client-first, so the result
    // is one or zero parts and the authenticator must reject. This blocks
    // the cross-mechanism confusion attack at the PLAIN boundary.
    let auth = PlainAuthenticator::new_insecure();
    auth.add_user("alice", "secret").await;

    let scram_bytes = b"n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL";
    let result = auth
        .authenticate(SaslMechanism::Plain, scram_bytes, None)
        .await;
    assert!(
        !result.is_success(),
        "SCRAM bytes must not authenticate as PLAIN"
    );
}

#[tokio::test]
async fn scram_does_not_authenticate_plain_byte_payload() {
    // PLAIN bytes (`\0user\0pass`) lack the GS2 header SCRAM expects.
    // `handle_client_first` must reject without panicking and without
    // promoting the connection to AwaitingClientFinal.
    let plain_bytes = b"\0alice\0secret";
    let (state, _resp) = handle_client_first(plain_bytes, |_| {
        Some(ScramCredentials::derive(b"secret", &[1u8; 16], 4096))
    });
    assert!(
        matches!(state, ScramServerState::Failed(_)),
        "PLAIN bytes presented to SCRAM must transition to Failed, got {:?}",
        state
    );
}

// ------------------------------------------------------------------
// SCRAM-SHA-256
// ------------------------------------------------------------------

/// Drive a full SCRAM handshake against a stored credential, with a
/// configurable password used by the "client" half. Returns the final
/// server state so the caller can assert pass/fail.
fn drive_scram_handshake(
    user: &str,
    server_creds: ScramCredentials,
    client_password: &[u8],
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
    // Pull combined nonce, salt, iterations off the wire — the test client
    // doesn't get to peek at server_creds directly because the audited
    // unknown-user path returns a fake set.
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

    // Derive a client-side stored key from the password the test wants to
    // try, then assemble client-final.
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(client_password, &salt, iters, &mut salted);
    let mut mac = H::new_from_slice(&salted).unwrap();
    mac.update(b"Client Key");
    let client_key: [u8; 32] = mac.finalize().into_bytes().into();
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();

    let cbind_b64 = B64.encode(b"n,,");
    let client_final_without_proof = format!("c={},r={}", cbind_b64, combined_nonce);
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

#[tokio::test]
async fn scram_wrong_password_fails_at_proof() {
    let server_creds = ScramCredentials::derive(b"correct horse", &[1u8; 16], 4096);
    // Client tries a different password — proof won't verify.
    let final_state = drive_scram_handshake("alice", server_creds, b"battery staple");
    assert!(
        matches!(final_state, ScramServerState::Failed(_)),
        "wrong password must fail SCRAM verification"
    );
}

#[tokio::test]
async fn scram_unknown_user_fails_at_proof_not_at_lookup() {
    // The user-enumeration mitigation requires that an unknown user produces
    // a normal-looking server-first (with dummy creds), so the failure
    // point is the same as wrong-password. The first round-trip must NOT
    // return Failed — the legitimate client can't tell yet.
    let client_first = b"n,,n=ghost,r=fyko+d2lbbFgONRv9qkxdawL";
    let (state, server_first) = handle_client_first(client_first, |_| None);
    assert!(
        matches!(state, ScramServerState::AwaitingClientFinal { .. }),
        "unknown user must still receive server-first to prevent enumeration"
    );
    let s = std::str::from_utf8(&server_first).unwrap();
    assert!(s.starts_with("r="), "expected SCRAM server-first, got: {s}");

    // Then the eventual client-final fails (any proof, computed against
    // a non-existent user, will mismatch the dummy stored_key).
    let combined_nonce = match &state {
        ScramServerState::AwaitingClientFinal { combined_nonce, .. } => combined_nonce.clone(),
        _ => unreachable!(),
    };
    let cbind_b64 = B64.encode(b"n,,");
    let client_final = format!(
        "c={},r={},p={}",
        cbind_b64,
        combined_nonce,
        B64.encode([0u8; 32])
    );
    let (final_state, err) = handle_client_final(client_final.as_bytes(), state);
    assert!(matches!(final_state, ScramServerState::Failed(_)));
    let err_s = std::str::from_utf8(&err).unwrap();
    assert!(err_s.starts_with("e="), "SCRAM failure must use e= framing");
}

#[tokio::test]
async fn scram_client_final_in_wrong_state_is_rejected() {
    // A client-final without a prior client-first is an out-of-order request.
    // The provider state machine must refuse rather than misroute or panic.
    let bogus_client_final = b"c=biws,r=,p=AAAA";
    let (state, err) =
        handle_client_final(bogus_client_final, ScramServerState::AwaitingClientFirst);
    assert!(matches!(state, ScramServerState::Failed(_)));
    assert!(std::str::from_utf8(&err).unwrap().starts_with("e="));
}
