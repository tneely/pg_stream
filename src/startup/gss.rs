//! GSSAPI / SSPI authentication plumbing (wire framing only).

/// Supplies GSSAPI/SSPI security tokens, plugging a platform Kerberos/SSPI
/// library (e.g. `libgssapi` or Windows SSPI) into the wire exchange.
pub trait GssProvider {
    /// The error type produced by the underlying GSSAPI/SSPI implementation.
    type Error: std::fmt::Display;

    /// Processes the server's token (empty on the first call) and returns the
    /// next client token; repeats until the server replies `AuthenticationOk`.
    fn step(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error>;
}
