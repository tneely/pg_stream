//! SCRAM-SHA-256 authentication implementation.
//!
//! Implements the Salted Challenge Response Authentication Mechanism (SCRAM)
//! as specified in RFC 5802, using SHA-256 as the hash function.
//!
//! # Protocol Overview
//!
//! ```text
//! Client                                  Server
//!   |                                        |
//!   |-- client-first: n,,n=user,r=nonce ---->|
//!   |                                        |
//!   |<-- server-first: r=nonce+server,s=salt,i=iterations
//!   |                                        |
//!   |-- client-final: c=biws,r=nonce,p=proof |
//!   |                                        |
//!   |<-- server-final: v=verifier -----------|
//! ```

use std::num::NonZeroU32;

use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

/// Maximum server-selected PBKDF2 iteration count accepted by the client.
///
/// PostgreSQL currently defaults to 4,096 iterations. This upper bound leaves
/// substantial room for stronger server policies while bounding authentication
/// CPU work from an untrusted peer.
pub const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

/// Errors that can occur during SCRAM authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramError {
    /// Server's nonce doesn't start with client's nonce.
    InvalidServerNonce,
    /// Failed to parse server-first message.
    InvalidServerFirst(String),
    /// Failed to parse server-final message.
    InvalidServerFinal(String),
    /// Server signature verification failed.
    ServerSignatureMismatch,
    /// Base64 decoding failed.
    Base64Error(String),
    /// Invalid iteration count.
    InvalidIterationCount,
    /// The server certificate could not be parsed for channel binding.
    InvalidCertificate,
    /// The server did not offer a channel-binding mechanism but the client
    /// required one.
    ChannelBindingRequired,
}

/// Channel binding configuration for a SCRAM exchange.
///
/// Selects the GS2 header and `c=` attribute per RFC 5802 / RFC 5929.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelBinding {
    /// Client does not support channel binding (not over TLS). GS2 flag `n`.
    NotSupported,
    /// Client supports channel binding but the server offered only the
    /// non-PLUS mechanism; sent for downgrade detection. GS2 flag `y`.
    NotUsed,
    /// Bind to the `tls-server-end-point` data (hash of the server cert). Used
    /// with `SCRAM-SHA-256-PLUS`. GS2 flag `p=tls-server-end-point`.
    TlsServerEndPoint(Vec<u8>),
}

impl ChannelBinding {
    /// The GS2 header (channel-binding flag plus empty authzid).
    fn gs2_header(&self) -> Vec<u8> {
        match self {
            ChannelBinding::NotSupported => b"n,,".to_vec(),
            ChannelBinding::NotUsed => b"y,,".to_vec(),
            ChannelBinding::TlsServerEndPoint(_) => b"p=tls-server-end-point,,".to_vec(),
        }
    }

    /// The base64-encoded `c=` attribute: GS2 header followed by the binding
    /// data (empty unless a real binding is in use).
    fn cbind_input_b64(&self) -> String {
        let mut input = self.gs2_header();
        if let ChannelBinding::TlsServerEndPoint(data) = self {
            input.extend_from_slice(data);
        }
        BASE64.encode(input)
    }
}

/// Computes `tls-server-end-point` channel binding data (RFC 5929) for a DER
/// server certificate: the cert hashed with its signature algorithm's hash
/// (MD5/SHA-1 upgraded to SHA-256). Feed to [`ChannelBinding::TlsServerEndPoint`].
pub fn tls_server_end_point(cert_der: &[u8]) -> Result<Vec<u8>, ScramError> {
    let oid = cert_signature_oid(cert_der).ok_or(ScramError::InvalidCertificate)?;
    let alg = digest_for_signature_oid(&oid);
    Ok(digest::digest(alg, cert_der).as_ref().to_vec())
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::InvalidServerNonce => {
                write!(f, "server nonce does not contain client nonce prefix")
            }
            ScramError::InvalidServerFirst(msg) => {
                write!(f, "failed to parse server-first message: {msg}")
            }
            ScramError::InvalidServerFinal(msg) => {
                write!(f, "failed to parse server-final message: {msg}")
            }
            ScramError::ServerSignatureMismatch => write!(f, "server signature mismatch"),
            ScramError::Base64Error(msg) => write!(f, "base64 error: {msg}"),
            ScramError::InvalidIterationCount => write!(f, "invalid iteration count"),
            ScramError::InvalidCertificate => {
                write!(f, "could not parse server certificate for channel binding")
            }
            ScramError::ChannelBindingRequired => {
                write!(f, "server did not offer a channel-binding mechanism")
            }
        }
    }
}

impl std::error::Error for ScramError {}

/// SCRAM-SHA-256 client state machine.
///
/// Implements the client side of the SCRAM authentication protocol.
///
/// # Example
///
/// ```rust
/// use pg_stream::auth::ScramClient;
///
/// let mut client = ScramClient::new("username", "password");
///
/// // Send client-first message to server
/// let client_first = client.client_first();
///
/// // Receive server-first from server
/// // let server_first = "...";
/// // let client_final = client.client_final(&server_first)?;
///
/// // Send client-final to server, receive server-final
/// // client.verify_server(&server_final)?;
/// ```
pub struct ScramClient {
    username: String,
    password: String,
    channel_binding: ChannelBinding,
    client_nonce: String,
    client_first_bare: String,
    server_first: Option<String>,
    auth_message: Option<String>,
    server_key: Option<[u8; 32]>,
}

impl ScramClient {
    /// Creates a new SCRAM client without channel binding (`SCRAM-SHA-256`).
    ///
    /// # Arguments
    ///
    /// * `username` - The database username
    /// * `password` - The user's password
    pub fn new(username: &str, password: &str) -> Self {
        Self::with_channel_binding(username, password, ChannelBinding::NotSupported)
    }

    /// Creates a new SCRAM client with the given [`ChannelBinding`]
    /// (`TlsServerEndPoint` selects `SCRAM-SHA-256-PLUS`).
    pub fn with_channel_binding(
        username: &str,
        password: &str,
        channel_binding: ChannelBinding,
    ) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            channel_binding,
            client_nonce: generate_nonce(),
            client_first_bare: String::new(),
            server_first: None,
            auth_message: None,
            server_key: None,
        }
    }

    /// Creates a new SCRAM client with a specific nonce (for testing).
    #[cfg(test)]
    pub fn with_nonce(username: &str, password: &str, nonce: &str) -> Self {
        Self::with_nonce_and_binding(username, password, nonce, ChannelBinding::NotSupported)
    }

    /// Creates a SCRAM client with a fixed nonce and channel binding (testing).
    #[cfg(test)]
    pub fn with_nonce_and_binding(
        username: &str,
        password: &str,
        nonce: &str,
        channel_binding: ChannelBinding,
    ) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            channel_binding,
            client_nonce: nonce.to_string(),
            client_first_bare: String::new(),
            server_first: None,
            auth_message: None,
            server_key: None,
        }
    }

    /// Returns the SCRAM mechanism name for this client's channel binding.
    pub fn mechanism(&self) -> &'static str {
        match self.channel_binding {
            ChannelBinding::TlsServerEndPoint(_) => "SCRAM-SHA-256-PLUS",
            _ => "SCRAM-SHA-256",
        }
    }

    /// Generates the client-first message.
    ///
    /// This is the first message sent by the client to initiate SCRAM authentication.
    /// Format: `<gs2-header>n=username,r=client_nonce`
    pub fn client_first(&mut self) -> String {
        let gs2_header = String::from_utf8(self.channel_binding.gs2_header())
            .expect("GS2 header is valid UTF-8");

        // Username is SASLprepped and escaped (= -> =3D, , -> =2C).
        let escaped_username = escape_username(&self.username);
        self.client_first_bare = format!("n={},r={}", escaped_username, self.client_nonce);

        format!("{}{}", gs2_header, self.client_first_bare)
    }

    /// Processes the server-first message and generates the client-final message.
    ///
    /// # Arguments
    ///
    /// * `server_first` - The server-first message received from the server
    ///
    /// # Returns
    ///
    /// The client-final message to send to the server.
    pub fn client_final(&mut self, server_first: &str) -> Result<String, ScramError> {
        self.server_first = Some(server_first.to_string());

        // Parse server-first message: r=nonce,s=salt,i=iterations
        let (server_nonce, salt, iterations) = parse_server_first(server_first)?;

        // Verify server nonce starts with client nonce
        if !server_nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::InvalidServerNonce);
        }

        // Decode base64 salt
        let salt_bytes = BASE64
            .decode(&salt)
            .map_err(|e| ScramError::Base64Error(e.to_string()))?;

        // Compute salted password using PBKDF2
        let salted_password = pbkdf2_sha256(self.password.as_bytes(), &salt_bytes, iterations);

        // Compute client and server keys
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        self.server_key = Some(server_key);

        // Compute stored key: H(ClientKey)
        let stored_key = sha256(&client_key);

        // c= is base64(GS2 header ++ channel binding data).
        let client_final_without_proof = format!(
            "c={},r={}",
            self.channel_binding.cbind_input_b64(),
            server_nonce
        );

        // Build auth message
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_without_proof
        );
        self.auth_message = Some(auth_message.clone());

        // Compute client signature: HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());

        // Compute client proof: ClientKey XOR ClientSignature
        let client_proof = xor_bytes(&client_key, &client_signature);
        let client_proof_b64 = BASE64.encode(client_proof);

        Ok(format!(
            "{},p={}",
            client_final_without_proof, client_proof_b64
        ))
    }

    /// Verifies the server-final message.
    ///
    /// # Arguments
    ///
    /// * `server_final` - The server-final message received from the server
    pub fn verify_server(&self, server_final: &str) -> Result<(), ScramError> {
        let server_key = self
            .server_key
            .as_ref()
            .ok_or(ScramError::InvalidServerFinal(
                "client_final not called".into(),
            ))?;
        let auth_message = self
            .auth_message
            .as_ref()
            .ok_or(ScramError::InvalidServerFinal(
                "client_final not called".into(),
            ))?;

        // Parse server-final: v=verifier
        let server_signature_b64 = parse_server_final(server_final)?;
        let server_signature = BASE64
            .decode(&server_signature_b64)
            .map_err(|e| ScramError::Base64Error(e.to_string()))?;

        // Compute expected server signature: HMAC(ServerKey, AuthMessage)
        let expected_signature = hmac_sha256(server_key, auth_message.as_bytes());

        if server_signature != expected_signature {
            return Err(ScramError::ServerSignatureMismatch);
        }

        Ok(())
    }
}

/// Generates a random nonce for SCRAM authentication.
fn generate_nonce() -> String {
    let mut bytes = [0u8; 18];
    rand::fill(&mut bytes).expect("random generation failed");
    BASE64.encode(bytes)
}

/// Escapes a username for SCRAM.
/// `=` becomes `=3D`, `,` becomes `=2C`
fn escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Parses the server-first message.
/// Format: `r=nonce,s=salt,i=iterations`
fn parse_server_first(msg: &str) -> Result<(String, String, NonZeroU32), ScramError> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;

    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("s=") {
            salt = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("i=") {
            let count = value
                .parse::<u32>()
                .map_err(|_| ScramError::InvalidIterationCount)?;
            iterations = Some(
                NonZeroU32::new(count)
                    .filter(|count| count.get() <= MAX_SCRAM_ITERATIONS)
                    .ok_or(ScramError::InvalidIterationCount)?,
            );
        }
    }

    match (nonce, salt, iterations) {
        (Some(n), Some(s), Some(i)) => Ok((n, s, i)),
        _ => Err(ScramError::InvalidServerFirst(
            "missing required fields".into(),
        )),
    }
}

/// Parses the server-final message.
/// Format: `v=verifier`
fn parse_server_final(msg: &str) -> Result<String, ScramError> {
    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix("v=") {
            return Ok(value.to_string());
        }
    }
    Err(ScramError::InvalidServerFinal(
        "missing verifier field".into(),
    ))
}

/// Reads a DER TLV at `data[0..]`, returning `(tag, contents, rest)`.
///
/// Handles short and long definite-length forms; rejects indefinite length.
fn der_read_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let tag = *data.first()?;
    let first_len = *data.get(1)?;
    let (len, header) = if first_len & 0x80 == 0 {
        (first_len as usize, 2)
    } else {
        let n = (first_len & 0x7f) as usize;
        if n == 0 || n > 4 {
            return None; // indefinite or absurdly long
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | *data.get(2 + i)? as usize;
        }
        (len, 2 + n)
    };
    let contents = data.get(header..header + len)?;
    Some((tag, contents, &data[header + len..]))
}

/// Extracts the signatureAlgorithm OID from a DER X.509 certificate
/// (`SEQUENCE { tbsCertificate, signatureAlgorithm { OID, .. }, .. }`).
fn cert_signature_oid(cert_der: &[u8]) -> Option<Vec<u8>> {
    const SEQUENCE: u8 = 0x30;
    const OID: u8 = 0x06;

    let (tag, cert_body, _) = der_read_tlv(cert_der)?;
    if tag != SEQUENCE {
        return None;
    }
    // Skip tbsCertificate, then read signatureAlgorithm.
    let (_, _tbs, after_tbs) = der_read_tlv(cert_body)?;
    let (tag, sig_alg, _) = der_read_tlv(after_tbs)?;
    if tag != SEQUENCE {
        return None;
    }
    let (tag, oid, _) = der_read_tlv(sig_alg)?;
    if tag != OID {
        return None;
    }
    Some(oid.to_vec())
}

/// Maps a signature algorithm OID to its channel-binding digest. Per RFC 5929
/// MD5/SHA-1 upgrade to SHA-256; unknown OIDs default to SHA-256 like Postgres.
fn digest_for_signature_oid(oid: &[u8]) -> &'static digest::Algorithm {
    // OID bytes after the DER tag/len, i.e. the value only.
    // sha384WithRSAEncryption 1.2.840.113549.1.1.12
    const SHA384_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c];
    // sha512WithRSAEncryption 1.2.840.113549.1.1.13
    const SHA512_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d];
    // ecdsa-with-SHA384 1.2.840.10045.4.3.3
    const SHA384_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03];
    // ecdsa-with-SHA512 1.2.840.10045.4.3.4
    const SHA512_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04];

    match oid {
        SHA384_RSA | SHA384_ECDSA => &digest::SHA384,
        SHA512_RSA | SHA512_ECDSA => &digest::SHA512,
        _ => &digest::SHA256,
    }
}

/// Computes PBKDF2-HMAC-SHA256.
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: NonZeroU32) -> [u8; 32] {
    let mut result = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        password,
        &mut result,
    );
    result
}

/// Computes HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&hmac_key, data);
    tag.as_ref().try_into().expect("HMAC-SHA256 is 32 bytes")
}

/// Computes SHA256.
fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = digest::digest(&digest::SHA256, data);
    digest.as_ref().try_into().expect("SHA256 is 32 bytes")
}

/// XORs two byte arrays.
fn xor_bytes(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = a[i] ^ b[i];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_username() {
        assert_eq!(escape_username("user"), "user");
        assert_eq!(escape_username("user=test"), "user=3Dtest");
        assert_eq!(escape_username("user,test"), "user=2Ctest");
        assert_eq!(escape_username("a=b,c"), "a=3Db=2Cc");
    }

    #[test]
    fn test_parse_server_first() {
        let msg = "r=clientnonce+servernonce,s=c2FsdA==,i=4096";
        let (nonce, salt, iterations) = parse_server_first(msg).unwrap();
        assert_eq!(nonce, "clientnonce+servernonce");
        assert_eq!(salt, "c2FsdA==");
        assert_eq!(iterations.get(), 4096);
    }

    #[test]
    fn test_parse_server_first_rejects_unbounded_work() {
        for iterations in [0, MAX_SCRAM_ITERATIONS + 1] {
            let msg = format!("r=nonce,s=c2FsdA==,i={iterations}");
            assert!(matches!(
                parse_server_first(&msg),
                Err(ScramError::InvalidIterationCount)
            ));
        }
    }

    #[test]
    fn test_parse_server_final() {
        let msg = "v=cm1GM3pydXVYNWhKNDZlcm5yL2RLbTdrSzg0cXdqRS8=";
        let verifier = parse_server_final(msg).unwrap();
        assert_eq!(verifier, "cm1GM3pydXVYNWhKNDZlcm5yL2RLbTdrSzg0cXdqRS8=");
    }

    #[test]
    fn test_client_first() {
        let mut client = ScramClient::with_nonce("user", "password", "rOprNGfwEbeRWgbNEkqO");
        let client_first = client.client_first();

        // Should start with GS2 header
        assert!(client_first.starts_with("n,,"));
        // Should contain username
        assert!(client_first.contains("n=user"));
        // Should contain nonce
        assert!(client_first.contains("r=rOprNGfwEbeRWgbNEkqO"));
    }

    #[test]
    fn test_scram_full_flow() {
        // Test vector from RFC 5802 adapted for SHA-256
        let mut client = ScramClient::with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");

        // Step 1: Client first
        let client_first = client.client_first();
        assert_eq!(client_first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

        // Step 2: Server first (simulated)
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

        // Step 3: Client final
        let client_final = client.client_final(server_first).unwrap();
        assert!(client_final.starts_with("c=biws,r="));
        assert!(client_final.contains(",p="));
    }

    #[test]
    fn test_invalid_server_nonce() {
        let mut client = ScramClient::with_nonce("user", "password", "clientnonce");
        client.client_first();

        // Server nonce doesn't start with client nonce
        let server_first = "r=differentnonce,s=c2FsdA==,i=4096";
        let result = client.client_final(server_first);
        assert!(matches!(result, Err(ScramError::InvalidServerNonce)));
    }

    #[test]
    fn test_mechanism_name_by_binding() {
        assert_eq!(ScramClient::new("u", "p").mechanism(), "SCRAM-SHA-256");
        let plus = ScramClient::with_channel_binding(
            "u",
            "p",
            ChannelBinding::TlsServerEndPoint(vec![0u8; 32]),
        );
        assert_eq!(plus.mechanism(), "SCRAM-SHA-256-PLUS");
    }

    #[test]
    fn test_gs2_header_and_cbind_encoding() {
        // No channel binding: "n,," -> base64 "biws".
        let mut c =
            ScramClient::with_nonce_and_binding("u", "p", "nonce", ChannelBinding::NotSupported);
        assert!(c.client_first().starts_with("n,,"));
        assert_eq!(ChannelBinding::NotSupported.cbind_input_b64(), "biws");

        // Supported but not used (downgrade guard): "y,," -> base64 "eSws".
        let mut c = ScramClient::with_nonce_and_binding("u", "p", "nonce", ChannelBinding::NotUsed);
        assert!(c.client_first().starts_with("y,,"));
        assert_eq!(ChannelBinding::NotUsed.cbind_input_b64(), "eSws");

        // tls-server-end-point: header + cert hash.
        let cb = ChannelBinding::TlsServerEndPoint(vec![0xAB; 4]);
        let mut c = ScramClient::with_nonce_and_binding("u", "p", "nonce", cb.clone());
        assert!(c.client_first().starts_with("p=tls-server-end-point,,"));
        let expected = BASE64.encode([b"p=tls-server-end-point,,".as_slice(), &[0xAB; 4]].concat());
        assert_eq!(cb.cbind_input_b64(), expected);
    }

    #[test]
    fn test_scram_plus_full_flow_binds_cert() {
        let cert_hash = vec![0x11u8; 32];
        let mut client = ScramClient::with_nonce_and_binding(
            "user",
            "pencil",
            "rOprNGfwEbeRWgbNEkqO",
            ChannelBinding::TlsServerEndPoint(cert_hash.clone()),
        );

        let client_first = client.client_first();
        assert_eq!(
            client_first,
            "p=tls-server-end-point,,n=user,r=rOprNGfwEbeRWgbNEkqO"
        );

        let server_first = "r=rOprNGfwEbeRWgbNEkqO-server,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final = client.client_final(server_first).unwrap();

        // c= must be base64("p=tls-server-end-point,," ++ cert_hash).
        let expected_cbind =
            BASE64.encode([b"p=tls-server-end-point,,".as_slice(), &cert_hash].concat());
        assert!(client_final.starts_with(&format!("c={expected_cbind},r=")));
        assert!(client_final.contains(",p="));
    }

    fn test_cert_der() -> Vec<u8> {
        let pem = include_str!("../../tests/data/certs/server.crt");
        let b64: String = pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .concat();
        BASE64.decode(b64).unwrap()
    }

    #[test]
    fn test_cert_signature_oid_and_binding_hash() {
        let der = test_cert_der();
        let oid = cert_signature_oid(&der).expect("should parse cert");
        // Test cert is sha256WithRSAEncryption: 1.2.840.113549.1.1.11.
        assert_eq!(oid, [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b]);

        // With SHA-256 signature, binding data is SHA-256 of the whole cert.
        let binding = tls_server_end_point(&der).unwrap();
        assert_eq!(binding, digest::digest(&digest::SHA256, &der).as_ref());
        assert_eq!(binding.len(), 32);
    }

    #[test]
    fn test_cert_parse_rejects_garbage() {
        assert!(cert_signature_oid(&[0x30, 0x02, 0xFF]).is_none());
        assert!(tls_server_end_point(b"not a cert").is_err());
    }
}
