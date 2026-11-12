//! Authentication requests.

use super::cursor::{Cursor, text};

/// An authentication request, or the news that authentication succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Authentication<'a> {
    /// Authentication succeeded.
    Ok,
    KerberosV5,
    CleartextPassword,
    Md5Password {
        salt: [u8; 4],
    },
    Gss,
    /// A GSSAPI or SSPI token to feed back into the security context.
    GssContinue(&'a [u8]),
    Sspi,
    /// SASL is required; these are the mechanisms on offer.
    Sasl(SaslMechanisms<'a>),
    /// A SASL challenge.
    SaslContinue(&'a [u8]),
    /// The SASL outcome data, sent just before [`Ok`](Self::Ok).
    SaslFinal(&'a [u8]),
    /// A request this crate has no name for.
    Unknown {
        code: u32,
        body: &'a [u8],
    },
}

impl<'a> Authentication<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let code = cursor.u32()?;
        let body = cursor.0;
        Some(match code {
            0 => Authentication::Ok,
            2 => Authentication::KerberosV5,
            3 => Authentication::CleartextPassword,
            5 => Authentication::Md5Password {
                salt: body.try_into().ok()?,
            },
            7 => Authentication::Gss,
            8 => Authentication::GssContinue(body),
            9 => Authentication::Sspi,
            10 => Authentication::Sasl(SaslMechanisms(body)),
            11 => Authentication::SaslContinue(body),
            12 => Authentication::SaslFinal(body),
            code => Authentication::Unknown { code, body },
        })
    }

    /// The AuthenticationRequest code on the wire.
    pub fn code(&self) -> u32 {
        match *self {
            Authentication::Ok => 0,
            Authentication::KerberosV5 => 2,
            Authentication::CleartextPassword => 3,
            Authentication::Md5Password { .. } => 5,
            Authentication::Gss => 7,
            Authentication::GssContinue(_) => 8,
            Authentication::Sspi => 9,
            Authentication::Sasl(_) => 10,
            Authentication::SaslContinue(_) => 11,
            Authentication::SaslFinal(_) => 12,
            Authentication::Unknown { code, .. } => code,
        }
    }
}

/// The SASL mechanisms a server offers, in its order of preference.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SaslMechanisms<'a>(&'a [u8]);

impl<'a> SaslMechanisms<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &'a str> + use<'a> {
        let names = self.0.split(|&b| b == 0);
        names.filter(|name| !name.is_empty()).map(text)
    }

    pub fn contains(&self, mechanism: &str) -> bool {
        self.iter().any(|name| name == mechanism)
    }
}

impl std::fmt::Debug for SaslMechanisms<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
