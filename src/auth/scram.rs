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

use aws_lc_rs::{digest, hmac, pbkdf2, rand};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

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
    client_nonce: String,
    client_first_bare: String,
    server_first: Option<String>,
    auth_message: Option<String>,
    server_key: Option<[u8; 32]>,
}

impl ScramClient {
    /// Creates a new SCRAM client.
    ///
    /// # Arguments
    ///
    /// * `username` - The database username
    /// * `password` - The user's password
    pub fn new(username: &str, password: &str) -> Self {
        let client_nonce = generate_nonce();
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce,
            client_first_bare: String::new(),
            server_first: None,
            auth_message: None,
            server_key: None,
        }
    }

    /// Creates a new SCRAM client with a specific nonce (for testing).
    #[cfg(test)]
    pub fn with_nonce(username: &str, password: &str, nonce: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: nonce.to_string(),
            client_first_bare: String::new(),
            server_first: None,
            auth_message: None,
            server_key: None,
        }
    }

    /// Returns the SCRAM mechanism name.
    pub fn mechanism() -> &'static str {
        "SCRAM-SHA-256"
    }

    /// Generates the client-first message.
    ///
    /// This is the first message sent by the client to initiate SCRAM authentication.
    /// Format: `n,,n=username,r=client_nonce`
    pub fn client_first(&mut self) -> String {
        // GS2 header: n,, (no channel binding, no authzid)
        let gs2_header = "n,,";

        // Client-first-bare: n=username,r=nonce
        // Username needs to be SASLprepped and escaped (= -> =3D, , -> =2C)
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

        // Build client-final-without-proof
        // c=biws is base64("n,,") - the GS2 header
        let client_final_without_proof = format!("c=biws,r={}", server_nonce);

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
fn parse_server_first(msg: &str) -> Result<(String, String, u32), ScramError> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;

    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("s=") {
            salt = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("i=") {
            iterations = Some(
                value
                    .parse::<u32>()
                    .map_err(|_| ScramError::InvalidIterationCount)?,
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

/// Computes PBKDF2-HMAC-SHA256.
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut result = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iterations.try_into().expect("iteration count too large"),
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
        assert_eq!(iterations, 4096);
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
}
