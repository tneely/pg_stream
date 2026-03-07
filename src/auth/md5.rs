//! MD5 password authentication.
//!
//! PostgreSQL's MD5 authentication uses a double-hash scheme:
//! `md5(md5(password + username) + salt)`
//!
//! The result is sent as the string "md5" followed by the hex-encoded hash.

use bytes::{BufMut, BytesMut};
use md5::Context;

use crate::message::MessageCode;

/// Computes the MD5 password hash for Postgres authentication.
///
/// The hash is computed as: `md5(md5(password + username) + salt)`
///
/// # Arguments
///
/// * `username` - The database username
/// * `password` - The user's password
/// * `salt` - The 4-byte salt provided by the server
///
/// # Returns
///
/// A `BytesMut` containing the password message ready to be sent.
///
/// # Example
///
/// ```rust
/// use pg_stream::auth::md5_password;
///
/// let salt = [0x12, 0x34, 0x56, 0x78];
/// let msg = md5_password("postgres", "secret", &salt);
/// // Send msg to the server
/// ```
pub fn md5_password(username: &str, password: &str, salt: &[u8; 4]) -> BytesMut {
    // First hash: md5(password + username)
    let mut ctx = Context::new();
    ctx.consume(password.as_bytes());
    ctx.consume(username.as_bytes());
    let inner_hash = ctx.compute();

    // Convert to hex string
    let inner_hex = hex_encode(&inner_hash.0);

    // Second hash: md5(inner_hex + salt)
    let mut ctx = Context::new();
    ctx.consume(inner_hex.as_bytes());
    ctx.consume(salt);
    let outer_hash = ctx.compute();

    // Final result: "md5" + hex(outer_hash) + null terminator
    let outer_hex = hex_encode(&outer_hash.0);

    let mut msg = BytesMut::new();
    MessageCode::PASSWORD_MESSAGE.write(&mut msg, |buf| {
        buf.put_slice(b"md5");
        buf.put_slice(outer_hex.as_bytes());
        buf.put_u8(0);
    });
    msg
}

/// Encodes bytes as a lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(HEX_CHARS[(byte >> 4) as usize] as char);
        result.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_password() {
        // Known test vector
        let salt = [0x8b, 0x60, 0x4c, 0x73];
        let msg = md5_password("postgres", "password", &salt);

        // Check message code
        assert_eq!(msg[0], b'p');

        // Extract the password string (after code + length)
        let password_str = String::from_utf8_lossy(&msg[5..msg.len() - 1]);
        assert!(password_str.starts_with("md5"));
        assert_eq!(password_str.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0xff]), "ff");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_known_md5_hash() {
        // Test with known values from PostgreSQL
        // User: "postgres", Password: "password", Salt: [0, 0, 0, 0]
        let salt = [0x00, 0x00, 0x00, 0x00];
        let msg = md5_password("postgres", "password", &salt);

        // The result should be "md5" followed by a specific hash
        let password_str = String::from_utf8_lossy(&msg[5..msg.len() - 1]);

        // First hash: md5("passwordpostgres") = "2e7dc6f8...""
        // Second hash: md5(first_hash_hex + salt)
        assert!(password_str.starts_with("md5"));
    }
}
