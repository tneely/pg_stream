//! SCRAM-SHA-256 (RFC 5802, RFC 7677), with optional channel binding
//! (RFC 5929).
//!
//! ```text
//! client-first   n,,n=,r=<client nonce>
//! server-first   r=<client nonce><server nonce>,s=<salt>,i=<iterations>
//! client-final   c=<channel binding>,r=<nonce>,p=<client proof>
//! server-final   v=<server signature>
//! ```

use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256, Sha384, Sha512};

use crate::ScramError;

/// The most PBKDF2 iterations a server may ask for. PostgreSQL defaults to
/// 4096; the cap bounds the CPU time an untrusted server can demand.
pub const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

/// How a SCRAM exchange is tied to the TLS connection underneath it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelBinding {
    /// The client cannot bind: there is no TLS, or no server certificate.
    Unsupported,
    /// The client could bind, but the server did not offer to. Saying so lets
    /// the server detect an attacker who stripped the offer.
    Unused,
    /// Bind to the server certificate, using the hash from
    /// [`tls_server_end_point`]. Selects `SCRAM-SHA-256-PLUS`.
    TlsServerEndPoint(Vec<u8>),
}

impl ChannelBinding {
    /// The GS2 header: the binding flag, then an empty authorization identity.
    fn gs2_header(&self) -> &'static str {
        match self {
            ChannelBinding::Unsupported => "n,,",
            ChannelBinding::Unused => "y,,",
            ChannelBinding::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        }
    }

    /// The `c=` attribute: the GS2 header followed by the binding data.
    fn attribute(&self) -> String {
        let mut input = self.gs2_header().as_bytes().to_vec();
        if let ChannelBinding::TlsServerEndPoint(hash) = self {
            input.extend_from_slice(hash);
        }
        BASE64.encode(input)
    }
}

/// A SCRAM exchange that has produced its first message and awaits the
/// server's.
pub struct Scram {
    password: String,
    binding: ChannelBinding,
    nonce: String,
    client_first: String,
}

impl Scram {
    /// Starts an exchange.
    ///
    /// The password is used as given: it is not normalized with SASLprep, so
    /// a password containing non-ASCII characters only matches if it was set
    /// in the same normalized form.
    pub fn new(password: &str, binding: ChannelBinding) -> Self {
        let mut nonce = [0u8; 18];
        getrandom::fill(&mut nonce).expect("the operating system provides randomness");
        Self::with_nonce(password, binding, &BASE64.encode(nonce))
    }

    fn with_nonce(password: &str, binding: ChannelBinding, nonce: &str) -> Self {
        // PostgreSQL ignores the user name here and uses the startup packet's.
        let client_first = format!("{}n=,r={nonce}", binding.gs2_header());
        Self {
            password: password.into(),
            binding,
            nonce: nonce.into(),
            client_first,
        }
    }

    /// The SASL mechanism this exchange speaks.
    pub fn mechanism(&self) -> &'static str {
        match self.binding {
            ChannelBinding::TlsServerEndPoint(_) => "SCRAM-SHA-256-PLUS",
            ChannelBinding::Unsupported | ChannelBinding::Unused => "SCRAM-SHA-256",
        }
    }

    /// The client-first message, for the SASLInitialResponse.
    pub fn client_first(&self) -> &str {
        &self.client_first
    }

    /// Answers the server-first message with the client's proof.
    pub fn respond(self, server_first: &[u8]) -> Result<ScramProof, ScramError> {
        let server_first = std::str::from_utf8(server_first).or(Err(ScramError::InvalidMessage))?;
        let attribute = |name| attribute(server_first, name).ok_or(ScramError::InvalidMessage);

        let nonce = attribute("r=")?;
        if !nonce.starts_with(&self.nonce) || nonce.len() == self.nonce.len() {
            return Err(ScramError::InvalidNonce);
        }
        let salt = BASE64.decode(attribute("s=")?);
        let salt = salt.or(Err(ScramError::InvalidMessage))?;
        let iterations = match attribute("i=")?.parse() {
            Ok(iterations @ 1..=MAX_SCRAM_ITERATIONS) => iterations,
            _ => return Err(ScramError::InvalidIterationCount),
        };

        let salted_password = pbkdf2(self.password.as_bytes(), &salt, iterations);
        let client_key = hmac(&salted_password, b"Client Key");
        let server_key = hmac(&salted_password, b"Server Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();

        // Everything both sides have said so far, minus the GS2 header and
        // the proof itself.
        let client_first_bare = &self.client_first[self.binding.gs2_header().len()..];
        let without_proof = format!("c={},r={nonce}", self.binding.attribute());
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");

        let mut proof = hmac(&stored_key, auth_message.as_bytes());
        xor(&mut proof, &client_key);

        Ok(ScramProof {
            client_final: format!("{without_proof},p={}", BASE64.encode(proof)),
            server_signature: hmac(&server_key, auth_message.as_bytes()),
        })
    }
}

impl fmt::Debug for Scram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The password is left out.
        let mut debug = f.debug_struct("Scram");
        debug
            .field("mechanism", &self.mechanism())
            .finish_non_exhaustive()
    }
}

/// A SCRAM exchange that has proven the client's identity and awaits the
/// server's proof in return.
pub struct ScramProof {
    client_final: String,
    server_signature: [u8; 32],
}

impl ScramProof {
    /// The client-final message, for the SASLResponse.
    pub fn client_final(&self) -> &str {
        &self.client_final
    }

    /// Checks the server-final message, which proves the server knows the
    /// password too.
    pub fn verify(self, server_final: &[u8]) -> Result<(), ScramError> {
        let server_final = std::str::from_utf8(server_final).or(Err(ScramError::InvalidMessage))?;
        let signature = attribute(server_final, "v=").ok_or(ScramError::InvalidMessage)?;
        let signature = BASE64.decode(signature);
        let signature = signature.or(Err(ScramError::InvalidMessage))?;

        // Compared without short-circuiting, so timing says nothing about how
        // much of a forged signature was right.
        let same_len = signature.len() == self.server_signature.len();
        let bytes = signature.iter().zip(self.server_signature);
        let difference = bytes.fold(0, |difference, (a, b)| difference | (a ^ b));
        match same_len && difference == 0 {
            true => Ok(()),
            false => Err(ScramError::ServerSignatureMismatch),
        }
    }
}

impl fmt::Debug for ScramProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramProof").finish_non_exhaustive()
    }
}

/// The value of the attribute starting with `name` (such as `"r="`) in a
/// comma-separated SCRAM message.
fn attribute<'a>(message: &'a str, name: &str) -> Option<&'a str> {
    let mut attributes = message.split(',');
    attributes.find_map(|attribute| attribute.strip_prefix(name))
}

fn xor(into: &mut [u8; 32], other: &[u8; 32]) {
    for (byte, other) in into.iter_mut().zip(other) {
        *byte ^= other;
    }
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// PBKDF2-HMAC-SHA-256 (RFC 8018) for a single output block, which is all
/// SCRAM needs: its key is as long as the hash.
fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let prf = Hmac::<Sha256>::new_from_slice(password).expect("HMAC takes a key of any length");

    let mut mac = prf.clone();
    mac.update(salt);
    mac.update(&1u32.to_be_bytes());
    let mut block: [u8; 32] = mac.finalize().into_bytes().into();

    let mut result = block;
    for _ in 1..iterations {
        let mut mac = prf.clone();
        mac.update(&block);
        block = mac.finalize().into_bytes().into();
        xor(&mut result, &block);
    }
    result
}

/// Computes the `tls-server-end-point` channel binding data for a server
/// certificate in DER form: the certificate hashed with the hash of its
/// signature algorithm, or SHA-256 where that is MD5 or SHA-1.
pub fn tls_server_end_point(certificate: &[u8]) -> Result<Vec<u8>, ScramError> {
    // sha384WithRSAEncryption, sha512WithRSAEncryption, ecdsa-with-SHA384 and
    // ecdsa-with-SHA512, as DER-encoded OID contents.
    const SHA384_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c];
    const SHA512_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d];
    const SHA384_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03];
    const SHA512_ECDSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04];

    let algorithm = signature_algorithm(certificate).ok_or(ScramError::InvalidCertificate)?;
    Ok(match algorithm {
        SHA384_RSA | SHA384_ECDSA => Sha384::digest(certificate).to_vec(),
        SHA512_RSA | SHA512_ECDSA => Sha512::digest(certificate).to_vec(),
        _ => Sha256::digest(certificate).to_vec(),
    })
}

/// Finds the signature algorithm OID of an X.509 certificate:
/// `SEQUENCE { tbsCertificate, SEQUENCE { OID, .. }, .. }`.
fn signature_algorithm(certificate: &[u8]) -> Option<&[u8]> {
    const SEQUENCE: u8 = 0x30;
    const OID: u8 = 0x06;

    let (SEQUENCE, certificate, _) = der_element(certificate)? else {
        return None;
    };
    let (_, _tbs_certificate, rest) = der_element(certificate)?;
    let (SEQUENCE, algorithm, _) = der_element(rest)? else {
        return None;
    };
    let (OID, oid, _) = der_element(algorithm)? else {
        return None;
    };
    Some(oid)
}

/// Splits one DER element off the front of `data`: its tag, its contents, and
/// what follows it.
fn der_element(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&[tag, first], rest) = data.split_first_chunk::<2>()?;
    let (len, rest) = match first {
        // Short form: the byte is the length.
        0..0x80 => (first as usize, rest),
        // Long form: the low bits count the length bytes that follow.
        _ => {
            let (len, rest) = rest.split_at_checked((first & 0x7f) as usize)?;
            if len.is_empty() || len.len() > 4 {
                return None;
            }
            (
                len.iter().fold(0, |len, &byte| (len << 8) | byte as usize),
                rest,
            )
        }
    };
    let (contents, rest) = rest.split_at_checked(len)?;
    Some((tag, contents, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client-first with the user name the RFC's example uses, which
    /// PostgreSQL clients leave empty.
    fn rfc_client(password: &str, nonce: &str) -> Scram {
        let mut scram = Scram::with_nonce(password, ChannelBinding::Unsupported, nonce);
        scram.client_first = format!("n,,n=user,r={nonce}");
        scram
    }

    /// Why a client with the nonce `nonce` refuses a server-first message.
    fn rejection(server_first: &[u8]) -> Option<ScramError> {
        let scram = rfc_client("pw", "nonce");
        scram.respond(server_first).err()
    }

    /// The worked example from RFC 7677 section 3.
    #[test]
    fn matches_the_rfc_7677_test_vector() {
        let scram = rfc_client("pencil", "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(scram.client_first(), "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        assert_eq!(scram.mechanism(), "SCRAM-SHA-256");

        let server_first = b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let client_final = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
            p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        let server_final = b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
        let proof = scram.respond(server_first).unwrap();
        assert_eq!(proof.client_final(), client_final);
        proof.verify(server_final).unwrap();
    }

    #[test]
    fn rejects_a_server_that_does_not_know_the_password() {
        let respond = || {
            let server_first = b"r=nonce+server,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
            rfc_client("pencil", "nonce").respond(server_first).unwrap()
        };

        for forged in [
            "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "v=AAAA",
            "v=",
        ] {
            let err = respond().verify(forged.as_bytes()).unwrap_err();
            assert_eq!(err, ScramError::ServerSignatureMismatch, "{forged}");
        }
        for malformed in ["e=other-error", "v=not base64", ""] {
            let err = respond().verify(malformed.as_bytes()).unwrap_err();
            assert_eq!(err, ScramError::InvalidMessage, "{malformed}");
        }
    }

    #[test]
    fn rejects_a_nonce_the_server_did_not_extend() {
        for nonce in ["other", "nonce"] {
            let server_first = format!("r={nonce},s=c2FsdA==,i=4096");
            let err = rejection(server_first.as_bytes());
            assert_eq!(err, Some(ScramError::InvalidNonce), "{nonce}");
        }
    }

    #[test]
    fn bounds_the_work_a_server_can_demand() {
        for iterations in ["0", "1000001", "-1", "many"] {
            let server_first = format!("r=nonce+server,s=c2FsdA==,i={iterations}");
            let err = rejection(server_first.as_bytes());
            assert_eq!(err, Some(ScramError::InvalidIterationCount), "{iterations}");
        }
    }

    #[test]
    fn rejects_malformed_server_first_messages() {
        for server_first in ["", "r=nonce+server,i=4096", "r=nonce+server,s=!!,i=4096"] {
            let err = rejection(server_first.as_bytes());
            assert_eq!(err, Some(ScramError::InvalidMessage), "{server_first}");
        }
        let err = rejection(&[0xff, 0xfe]);
        assert_eq!(err, Some(ScramError::InvalidMessage));
    }

    #[test]
    fn channel_binding_selects_the_header_and_mechanism() {
        let hash = vec![0xab; 32];
        let bound = ChannelBinding::TlsServerEndPoint(hash.clone());
        let cases = [
            (
                ChannelBinding::Unsupported,
                "n,,",
                "SCRAM-SHA-256",
                "biws".to_string(),
            ),
            (
                ChannelBinding::Unused,
                "y,,",
                "SCRAM-SHA-256",
                "eSws".to_string(),
            ),
            (
                bound,
                "p=tls-server-end-point,,",
                "SCRAM-SHA-256-PLUS",
                BASE64.encode([b"p=tls-server-end-point,,".as_slice(), &hash].concat()),
            ),
        ];

        for (binding, header, mechanism, attribute) in cases {
            let scram = Scram::with_nonce("pw", binding, "nonce");
            assert_eq!(scram.client_first(), format!("{header}n=,r=nonce"));
            assert_eq!(scram.mechanism(), mechanism);

            let proof = scram.respond(b"r=nonce+server,s=c2FsdA==,i=1").unwrap();
            let expected = format!("c={attribute},r=nonce+server,p=");
            assert!(
                proof.client_final().starts_with(&expected),
                "{}",
                proof.client_final()
            );
        }
    }

    #[test]
    fn nonces_are_random() {
        let first = Scram::new("pw", ChannelBinding::Unsupported);
        let second = Scram::new("pw", ChannelBinding::Unsupported);
        assert_ne!(first.client_first(), second.client_first());
    }

    fn test_certificate() -> Vec<u8> {
        let pem = include_str!("../../tests/data/certs/server.crt");
        let lines = pem.lines().filter(|line| !line.starts_with("-----"));
        BASE64.decode(lines.collect::<String>()).unwrap()
    }

    #[test]
    fn binds_to_a_certificate_with_its_signature_hash() {
        let certificate = test_certificate();
        // sha256WithRSAEncryption
        let algorithm = signature_algorithm(&certificate).unwrap();
        assert_eq!(
            algorithm,
            [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b]
        );

        let binding = tls_server_end_point(&certificate).unwrap();
        assert_eq!(binding, Sha256::digest(&certificate).to_vec());
    }

    #[test]
    fn rejects_data_that_is_not_a_certificate() {
        for garbage in [
            &b""[..],
            b"not a certificate",
            &[0x30, 0x02, 0xff],
            &[0x30, 0x84, 0xff],
        ] {
            assert_eq!(
                tls_server_end_point(garbage),
                Err(ScramError::InvalidCertificate)
            );
        }
    }
}
