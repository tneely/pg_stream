//! Authentication mechanisms.
//!
//! [`Startup`](crate::startup::Startup) drives these for you. They are public
//! for code that runs the authentication exchange itself.

mod md5;
mod scram;

pub use md5::md5_password;
pub use scram::{ChannelBinding, MAX_SCRAM_ITERATIONS, Scram, ScramProof, tls_server_end_point};

/// Produces GSSAPI or SSPI tokens.
///
/// Token generation needs a platform security library, so this crate carries
/// the exchange and leaves the tokens to an implementation of this trait.
pub trait GssProvider {
    /// Advances the security context with the server's latest token, which is
    /// empty on the first call, and returns the next token to send. An empty
    /// token means there is nothing to send.
    fn step(&mut self, token: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

impl<G: GssProvider + ?Sized> GssProvider for &mut G {
    fn step(&mut self, token: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        (**self).step(token)
    }
}
