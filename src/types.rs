//! Types shared by frontend and backend messages.

/// The wire format of a parameter or result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u16)]
pub enum Format {
    #[default]
    Text = 0,
    Binary = 1,
}

impl Format {
    pub(crate) fn from_code(code: u16) -> Option<Self> {
        match code {
            0 => Some(Format::Text),
            1 => Some(Format::Binary),
            _ => None,
        }
    }
}

/// A Postgres object identifier.
pub type Oid = u32;

/// OIDs of common built-in types, for Parse and FunctionCall.
///
/// See <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>.
pub mod oid {
    use super::Oid;

    /// Lets the server infer the type.
    pub const UNSPECIFIED: Oid = 0;
    pub const BOOL: Oid = 16;
    pub const BYTEA: Oid = 17;
    pub const CHAR: Oid = 18;
    pub const NAME: Oid = 19;
    pub const INT8: Oid = 20;
    pub const INT2: Oid = 21;
    pub const INT4: Oid = 23;
    pub const TEXT: Oid = 25;
    pub const OID: Oid = 26;
    pub const JSON: Oid = 114;
    pub const XML: Oid = 142;
    pub const FLOAT4: Oid = 700;
    pub const FLOAT8: Oid = 701;
    pub const MONEY: Oid = 790;
    pub const BPCHAR: Oid = 1042;
    pub const VARCHAR: Oid = 1043;
    pub const DATE: Oid = 1082;
    pub const TIME: Oid = 1083;
    pub const TIMESTAMP: Oid = 1114;
    pub const TIMESTAMPTZ: Oid = 1184;
    pub const INTERVAL: Oid = 1186;
    pub const TIMETZ: Oid = 1266;
    pub const NUMERIC: Oid = 1700;
    pub const UUID: Oid = 2950;
    pub const JSONB: Oid = 3802;
}

/// A protocol version, as sent in the startup packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    /// Protocol 3.0, spoken by every server since PostgreSQL 7.4.
    pub const V3_0: Self = Self::new(3, 0);

    /// Protocol 3.2 (PostgreSQL 18), which widens the cancellation key. Older
    /// servers answer with NegotiateProtocolVersion and stay on 3.0.
    pub const V3_2: Self = Self::new(3, 2);

    pub const fn new(major: u16, minor: u16) -> Self {
        Self(((major as u32) << 16) | minor as u32)
    }

    pub const fn major(self) -> u16 {
        (self.0 >> 16) as u16
    }

    pub const fn minor(self) -> u16 {
        self.0 as u16
    }

    pub(crate) const fn as_u32(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

/// A way for a client to prove who it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AuthMethod {
    /// The server asks for nothing: `trust`, or a method that does not involve
    /// the protocol, such as `peer` or `cert`.
    None,
    /// The password, sent as it is.
    Cleartext,
    /// The password, hashed with MD5 and a salt the server picks.
    Md5,
    /// SCRAM-SHA-256. The password is never sent, and the server has to prove
    /// that it knows it too.
    ScramSha256,
    /// SCRAM-SHA-256 bound to the TLS channel, which also proves that nobody
    /// sits between the client and that server.
    ScramSha256Plus,
    /// An OAuth 2.0 bearer token.
    OAuthBearer,
    Gss,
    Sspi,
}
