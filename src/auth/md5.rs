//! MD5 password authentication.

use md5::Context;

/// Computes the response to an AuthenticationMD5Password request:
/// `"md5" + md5(md5(password + user) + salt)`, with both digests in hex.
pub fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    let mut inner = Context::new();
    inner.consume(password);
    inner.consume(user);

    let mut outer = Context::new();
    outer.consume(format!("{:x}", inner.finalize()));
    outer.consume(salt);
    format!("md5{:x}", outer.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_the_password_then_the_salt() {
        let hashed = md5_password("postgres", "password", [0x8b, 0x60, 0x4c, 0x73]);
        assert_eq!(hashed.len(), 35);

        let inner = format!("{:x}", md5::compute("passwordpostgres"));
        assert_eq!(inner, "32e12f215ba27cb750c9e093ce4b5127");

        let mut salted = inner.into_bytes();
        salted.extend_from_slice(&[0x8b, 0x60, 0x4c, 0x73]);
        assert_eq!(hashed, format!("md5{:x}", md5::compute(salted)));
    }

    #[test]
    fn depends_on_every_input() {
        let base = md5_password("user", "pw", [1, 2, 3, 4]);
        assert_ne!(base, md5_password("other", "pw", [1, 2, 3, 4]));
        assert_ne!(base, md5_password("user", "other", [1, 2, 3, 4]));
        assert_ne!(base, md5_password("user", "pw", [4, 3, 2, 1]));
    }
}
