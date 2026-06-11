//! SCRAM-SHA-256 protocol primitives.
//!
//! Implements the parts of [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802)
//! we need to authenticate Kafka clients with `SCRAM-SHA-256`. Compared to
//! the PLAIN mechanism, SCRAM never sends the password (or anything from
//! which the password can be recovered) on the wire, even when the
//! transport is plaintext.
//!
//! ## Why this lives outside `sasl_provider.rs`
//!
//! The SCRAM math (PBKDF2, HMAC, SHA-256, Base64-armored client/server
//! messages) is independent of the user store and the per-connection
//! session map. Keeping it in its own module makes it unit-testable
//! against RFC vectors without dragging in tokio / config plumbing.
//!
//! ## Stored credentials
//!
//! For each user we store:
//!
//! - `salt` (16 random bytes)
//! - `iterations` (4096, Kafka's default minimum)
//! - `stored_key = SHA256(client_key)` where
//!   `client_key = HMAC(salted_password, "Client Key")`
//! - `server_key = HMAC(salted_password, "Server Key")`
//!
//! `salted_password = PBKDF2-HMAC-SHA256(password, salt, iterations, 32)`.
//! The password itself is *not* persisted in this struct — the call site
//! that has the cleartext password (e.g. `add_user`) calls
//! [`ScramCredentials::derive`] once and keeps only the result.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

type HmacSha256 = Hmac<Sha256>;

/// Length of a SHA-256 digest in bytes.
const SHA256_LEN: usize = 32;

/// Default iteration count for PBKDF2 — matches Kafka's default minimum.
/// Higher means more CPU at credential-derivation time but harder for an
/// offline attacker who has stolen `stored_key`.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Salt length in bytes — 16 is the SCRAM convention and matches Kafka.
pub const SALT_LEN: usize = 16;

/// Nonce length used for both client and server nonces (in bytes).
const NONCE_LEN: usize = 18;

/// Per-user SCRAM credential set, derived once at `add_user` time and
/// thereafter the only thing we keep about the password.
#[derive(Debug, Clone)]
pub struct ScramCredentials {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; SHA256_LEN],
    pub server_key: [u8; SHA256_LEN],
}

impl ScramCredentials {
    /// Derive credentials from a cleartext password using PBKDF2-HMAC-SHA256.
    ///
    /// The salt and iteration count are caller-supplied so credentials can
    /// be deterministic in tests. Production callers use [`Self::generate`].
    pub fn derive(password: &[u8], salt: &[u8], iterations: u32) -> Self {
        let salted = pbkdf2_hmac_sha256(password, salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let server_key = hmac_sha256(&salted, b"Server Key");
        let stored_key: [u8; SHA256_LEN] = Sha256::digest(client_key).into();
        Self {
            salt: salt.to_vec(),
            iterations,
            stored_key,
            server_key,
        }
    }

    /// Generate fresh credentials with a random salt and the default
    /// iteration count.
    pub fn generate(password: &[u8]) -> Self {
        let mut salt = [0u8; SALT_LEN];
        for byte in &mut salt {
            *byte = fastrand::u8(..);
        }
        Self::derive(password, &salt, DEFAULT_ITERATIONS)
    }
}

/// Stage of the SCRAM handshake from the server's perspective.
#[derive(Debug)]
pub enum ScramServerState {
    /// Awaiting the client-first-message.
    AwaitingClientFirst,
    /// Sent the server-first-message; awaiting the client-final-message.
    AwaitingClientFinal {
        /// The full client-first-message-bare (`n=user,r=clientnonce`).
        /// Required as input to `auth_message`.
        client_first_bare: String,
        /// The full server-first-message we sent.
        server_first: String,
        /// The combined nonce (clientnonce || servernonce).
        combined_nonce: String,
        /// The server-side credentials of the user the client claims to be.
        creds: ScramCredentials,
        /// The username extracted from the client-first-message.
        username: String,
    },
    /// Authentication complete (success). Holds the principal (`User:<name>`).
    Authenticated(String),
    /// Authentication failed. Holds the error reason for logging.
    Failed(String),
}

/// Parse a SCRAM `client-first-message` and produce the corresponding
/// `server-first-message`. The returned [`ScramServerState`] carries
/// everything the server needs to validate the upcoming
/// `client-final-message`.
///
/// On any parse / lookup failure returns `ScramServerState::Failed`.
pub fn handle_client_first(
    client_first: &[u8],
    creds_lookup: impl FnOnce(&str) -> Option<ScramCredentials>,
) -> (ScramServerState, Vec<u8>) {
    let s = match std::str::from_utf8(client_first) {
        Ok(s) => s,
        Err(_) => return fail("client-first not valid UTF-8"),
    };
    // GS2 header: `n,,` (no channel binding, no authzid). We accept both
    // `n,,` and `y,,`; the latter means the client supports channel binding
    // but the server doesn't — which is true for us.
    let (gs2_header, bare) = match s.split_once(",,") {
        Some((g, rest)) => (g, rest),
        None => return fail("client-first missing GS2 header"),
    };
    if gs2_header != "n" && gs2_header != "y" {
        return fail("client-first uses unsupported channel binding");
    }
    let mut user = None;
    let mut client_nonce = None;
    for attr in bare.split(',') {
        if let Some(rest) = attr.strip_prefix("n=") {
            user = Some(rest.to_string());
        } else if let Some(rest) = attr.strip_prefix("r=") {
            client_nonce = Some(rest.to_string());
        }
    }
    let username = match user {
        Some(u) if !u.is_empty() => u,
        _ => return fail("client-first missing username"),
    };
    let client_nonce = match client_nonce {
        Some(n) if !n.is_empty() => n,
        _ => return fail("client-first missing client nonce"),
    };
    let Some(creds) = creds_lookup(&username) else {
        // Don't reveal user existence — produce a server-first as if the
        // user existed, so the client only learns about the failure at the
        // proof-verification step. Avoids user enumeration.
        let dummy = ScramCredentials {
            salt: vec![0u8; SALT_LEN],
            iterations: DEFAULT_ITERATIONS,
            stored_key: [0u8; SHA256_LEN],
            server_key: [0u8; SHA256_LEN],
        };
        let server_nonce = random_nonce();
        let combined = format!("{}{}", client_nonce, server_nonce);
        let server_first = format!(
            "r={},s={},i={}",
            combined,
            B64.encode(&dummy.salt),
            dummy.iterations
        );
        let response = server_first.clone().into_bytes();
        return (
            ScramServerState::AwaitingClientFinal {
                client_first_bare: bare.to_string(),
                server_first,
                combined_nonce: combined,
                creds: dummy,
                username,
            },
            response,
        );
    };
    let server_nonce = random_nonce();
    let combined = format!("{}{}", client_nonce, server_nonce);
    let server_first = format!(
        "r={},s={},i={}",
        combined,
        B64.encode(&creds.salt),
        creds.iterations
    );
    let response = server_first.clone().into_bytes();
    (
        ScramServerState::AwaitingClientFinal {
            client_first_bare: bare.to_string(),
            server_first,
            combined_nonce: combined,
            creds,
            username,
        },
        response,
    )
}

/// Parse a SCRAM `client-final-message`, verify the proof, and produce
/// the `server-final-message`. Returns the new server state along with the
/// bytes to send back to the client. On verification failure the returned
/// state is `Failed` and the bytes contain a SCRAM `e=` error message.
pub fn handle_client_final(
    client_final: &[u8],
    state: ScramServerState,
) -> (ScramServerState, Vec<u8>) {
    use subtle::ConstantTimeEq;
    let ScramServerState::AwaitingClientFinal {
        client_first_bare,
        server_first,
        combined_nonce,
        creds,
        username,
    } = state
    else {
        return fail("client-final received in wrong SCRAM state");
    };
    let s = match std::str::from_utf8(client_final) {
        Ok(s) => s,
        Err(_) => return fail("client-final not valid UTF-8"),
    };
    let mut channel_binding = None;
    let mut nonce = None;
    // `client-final-message-without-proof` is everything before `,p=`. We
    // need to compute this exactly — the auth_message construction depends
    // on the on-the-wire byte sequence.
    let proof_split = s.rsplit_once(",p=");
    let (without_proof, proof_b64) = match proof_split {
        Some((wo, p)) => (wo, p),
        None => return fail("client-final missing proof"),
    };
    for attr in without_proof.split(',') {
        if let Some(rest) = attr.strip_prefix("c=") {
            channel_binding = Some(rest);
        } else if let Some(rest) = attr.strip_prefix("r=") {
            nonce = Some(rest);
        }
    }
    let Some(c) = channel_binding else {
        return fail("client-final missing channel binding");
    };
    // Expect base64 of "n,," (or "y,,") — match the GS2 header from
    // client-first. We accept either since both are commonly produced by
    // clients that don't actually do channel binding.
    let cb = match B64.decode(c) {
        Ok(b) => b,
        Err(_) => return fail("client-final channel binding not base64"),
    };
    if cb != b"n,," && cb != b"y,," {
        return fail("client-final channel binding mismatch");
    }
    let Some(n) = nonce else {
        return fail("client-final missing nonce");
    };
    if n != combined_nonce {
        return fail("client-final nonce mismatch");
    }
    let client_proof = match B64.decode(proof_b64) {
        Ok(p) if p.len() == SHA256_LEN => p,
        _ => return fail("client-final proof not 32 bytes base64"),
    };

    let auth_message = format!("{},{},{}", client_first_bare, server_first, without_proof);
    let client_signature = hmac_sha256(&creds.stored_key, auth_message.as_bytes());
    let mut client_key = [0u8; SHA256_LEN];
    for i in 0..SHA256_LEN {
        client_key[i] = client_proof[i] ^ client_signature[i];
    }
    let derived_stored_key: [u8; SHA256_LEN] = Sha256::digest(client_key).into();
    let valid: bool = derived_stored_key.ct_eq(&creds.stored_key).into();
    if !valid {
        // Client supplied a bad password OR the user didn't exist (we used
        // a zero stored_key in that case, which never matches a real
        // proof). Either way, return the same error.
        let err = b"e=invalid-proof".to_vec();
        return (ScramServerState::Failed("invalid proof".to_string()), err);
    }

    let server_signature = hmac_sha256(&creds.server_key, auth_message.as_bytes());
    let server_final = format!("v={}", B64.encode(server_signature));
    (
        ScramServerState::Authenticated(format!("User:{}", username)),
        server_final.into_bytes(),
    )
}

fn fail(msg: &str) -> (ScramServerState, Vec<u8>) {
    let err = format!("e={}", msg).into_bytes();
    (ScramServerState::Failed(msg.to_string()), err)
}

fn random_nonce() -> String {
    // RFC 5802 allows printable ASCII excluding `,` for nonces. We use
    // base64 of OS-random bytes (via uuid v4) which is alphanumeric +
    // `+/=` — standard SCRAM clients accept this.
    let nonce = uuid::Uuid::new_v4();
    B64.encode(nonce.as_bytes()).replace([',', '='], "")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; SHA256_LEN] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let out = mac.finalize().into_bytes();
    let mut result = [0u8; SHA256_LEN];
    result.copy_from_slice(&out);
    result
}

/// PBKDF2-HMAC-SHA256 producing exactly one 32-byte block. SCRAM-SHA-256
/// always derives a 32-byte salted password, so we hardcode `dkLen = 32`
/// and skip the multi-block loop.
fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; SHA256_LEN] {
    // U_1 = HMAC(password, salt || INT(1))
    let mut salt_with_idx = Vec::with_capacity(salt.len() + 4);
    salt_with_idx.extend_from_slice(salt);
    salt_with_idx.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha256(password, &salt_with_idx);
    let mut result = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for i in 0..SHA256_LEN {
            result[i] ^= u[i];
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pbkdf2_rfc6070_vector() {
        // RFC 6070 test vector for PBKDF2-HMAC-SHA1, adapted to SHA-256
        // by computing expected output independently. This vector verifies
        // basic PBKDF2 mechanics regardless of which HMAC backend is used.
        let pw = b"password";
        let salt = b"salt";
        let out = pbkdf2_hmac_sha256(pw, salt, 1);
        // Reference: PBKDF2-HMAC-SHA256(password, salt, 1, 32) =
        //   120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b
        let expected = [
            0x12, 0x0f, 0xb6, 0xcf, 0xfc, 0xf8, 0xb3, 0x2c, 0x43, 0xe7, 0x22, 0x52, 0x56, 0xc4,
            0xf8, 0x37, 0xa8, 0x65, 0x48, 0xc9, 0x2c, 0xcc, 0x35, 0x48, 0x08, 0x05, 0x98, 0x7c,
            0xb7, 0x0b, 0xe1, 0x7b,
        ];
        assert_eq!(out, expected);
    }

    #[test]
    fn derive_then_verify_roundtrip() {
        // Simulate the full SCRAM handshake against deterministic creds:
        // derive on the server, run client-first/final, verify proof.
        let password = b"hunter2";
        let creds = ScramCredentials::derive(password, b"sodium-chloride!", 4096);
        let user = "alice".to_string();

        // Client constructs client-first.
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = format!("n={},r={}", user, client_nonce);
        let client_first = format!("n,,{}", client_first_bare);

        // Server processes client-first.
        let creds_clone = creds.clone();
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

        // Extract combined nonce so the test "client" can echo it.
        let combined_nonce = server_first
            .split(',')
            .find_map(|a| a.strip_prefix("r="))
            .unwrap()
            .to_string();

        // Client computes proof.
        let salted = pbkdf2_hmac_sha256(password, &creds.salt, creds.iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let cbind_b64 = B64.encode(b"n,,");
        let client_final_without_proof = format!("c={},r={}", cbind_b64, combined_nonce);
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            B64.encode(client_proof)
        );

        // Server validates client-final.
        let (final_state, server_final_bytes) = handle_client_final(client_final.as_bytes(), state);
        match final_state {
            ScramServerState::Authenticated(p) => assert_eq!(p, "User:alice"),
            other => panic!("Expected Authenticated, got {:?}", other),
        }
        let server_final = std::str::from_utf8(&server_final_bytes).unwrap();
        assert!(server_final.starts_with("v="));
    }

    #[test]
    fn unknown_user_fails_at_proof() {
        // Unknown user gets a normal-looking server-first (with dummy
        // creds), but the proof check fails at client-final time. This is
        // the user-enumeration mitigation.
        let user = "alice";
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = format!("n={},r={}", user, client_nonce);
        let client_first = format!("n,,{}", client_first_bare);

        let (state, _server_first) =
            handle_client_first(client_first.as_bytes(), |_| None /* no users */);
        // State is AwaitingClientFinal even though the user doesn't exist.
        assert!(matches!(
            state,
            ScramServerState::AwaitingClientFinal { .. }
        ));

        // Construct a plausible-looking client-final with a bogus proof.
        let combined_nonce = match &state {
            ScramServerState::AwaitingClientFinal { combined_nonce, .. } => combined_nonce.clone(),
            _ => unreachable!(),
        };
        let cbind_b64 = B64.encode(b"n,,");
        let bogus_proof = B64.encode([0u8; 32]);
        let client_final = format!("c={},r={},p={}", cbind_b64, combined_nonce, bogus_proof);
        let (final_state, _err) = handle_client_final(client_final.as_bytes(), state);
        assert!(matches!(final_state, ScramServerState::Failed(_)));
    }

    #[test]
    fn nonce_mismatch_fails() {
        let user = "alice";
        let creds = ScramCredentials::derive(b"hunter2", &[1u8; 16], 4096);
        let client_nonce = "AAAAAAAAAAAAAAAAAAAA";
        let client_first_bare = format!("n={},r={}", user, client_nonce);
        let client_first = format!("n,,{}", client_first_bare);

        let creds_clone = creds.clone();
        let (state, _) =
            handle_client_first(client_first.as_bytes(), move |_| Some(creds_clone.clone()));
        let cbind_b64 = B64.encode(b"n,,");
        // Wrong nonce on client-final — must fail without revealing whether
        // the password was right.
        let client_final = format!(
            "c={},r=BBBBBBBBBBBBBBBBBBBB,p={}",
            cbind_b64,
            B64.encode([0u8; 32])
        );
        let (final_state, err) = handle_client_final(client_final.as_bytes(), state);
        assert!(matches!(final_state, ScramServerState::Failed(_)));
        assert!(std::str::from_utf8(&err).unwrap().starts_with("e="));
    }

    #[test]
    fn generate_creds_produce_unique_salts() {
        let a = ScramCredentials::generate(b"pw");
        let b = ScramCredentials::generate(b"pw");
        // Same password but different salts → different stored_key.
        assert_ne!(a.salt, b.salt);
        assert_ne!(a.stored_key, b.stored_key);
    }
}
