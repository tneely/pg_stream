//! Common types used in the Postgres wire protocol.

/// Parameter format codes for Bind messages.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

impl From<FormatCode> for u16 {
    fn from(value: FormatCode) -> Self {
        value as u16
    }
}

/// Postgres type OIDs.
///
/// See: <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
pub type Oid = u32;

/// Common Postgres type OIDs.
pub mod oid {
    use super::Oid;

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
