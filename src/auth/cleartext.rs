//! Cleartext password authentication.
//!
//! This is the simplest form of password authentication where the password
//! is sent as-is (null-terminated). Should only be used over TLS connections.

use bytes::{BufMut, BytesMut};

use crate::message::MessageCode;

/// Encodes a cleartext password message.
///
/// Returns a `BytesMut` containing the password message ready to be sent.
///
/// # Arguments
///
/// * `password` - The password to encode
///
/// # Example
///
/// ```rust
/// use pg_stream::auth::cleartext_password;
///
/// let msg = cleartext_password("my_secret_password");
/// // Send msg to the server
/// ```
pub fn cleartext_password(password: &str) -> BytesMut {
    let mut msg = BytesMut::new();
    MessageCode::PASSWORD_MESSAGE.write(&mut msg, |buf| {
        buf.put_slice(password.as_bytes());
        buf.put_u8(0);
    });
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleartext_password() {
        let msg = cleartext_password("secret");

        // Check message code
        assert_eq!(msg[0], b'p');
        // Check length (4 + "secret\0" = 4 + 7 = 11)
        let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(len, 11);
        // Check password content (including null terminator)
        assert_eq!(&msg[5..12], b"secret\0");
    }
}
