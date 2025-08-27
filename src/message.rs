pub mod frontend {
    //! Logic for handling and representing Postgres frontend messages.

    use bytes::{BufMut, BytesMut};

    /// Postgres frontend messages are framed by a 1 byte message code,
    /// followed by a u32 integer delineating the length of the rest of
    /// the message.
    ///
    /// The message code identifies the type of message and format of its
    /// payload.
    ///
    /// For more information, see the official Postgres docs:
    /// <https://www.postgresql.org/docs/current/protocol-message-formats.html>
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MessageCode(u8);

    impl MessageCode {
        pub const BIND: Self = Self(b'B');
        pub const CANCEL_REQUEST: Self = Self(16);
        pub const CLOSE: Self = Self(b'C');
        pub const COPY_DATA: Self = Self(b'd');
        pub const COPY_DONE: Self = Self(b'c');
        pub const COPY_FAIL: Self = Self(b'f');
        pub const DESCRIBE: Self = Self(b'D');
        pub const EXECUTE: Self = Self(b'E');
        pub const FLUSH: Self = Self(b'H');
        pub const FUNCTION_CALL: Self = Self(b'F');
        pub const GSSENC_REQUEST: Self = Self(8);
        pub const PARSE: Self = Self(b'P');
        pub const PASSWORD_MESSAGE: Self = Self(b'p');
        pub const QUERY: Self = Self(b'Q');
        pub const SASL_RESPONSE: Self = Self(b'p');
        pub const SYNC: Self = Self(b'S');
        pub const TERMINATE: Self = Self(b'X');

        #[inline]
        pub fn frame(self, buf: &mut BytesMut, payload_fn: impl FnOnce(&mut BytesMut)) {
            buf.put_u8(self.0);
            frame(buf, payload_fn);
        }
    }

    impl From<u8> for MessageCode {
        fn from(value: u8) -> Self {
            Self(value)
        }
    }

    impl From<MessageCode> for u8 {
        fn from(value: MessageCode) -> Self {
            value.0
        }
    }

    impl PartialEq<u8> for MessageCode {
        fn eq(&self, other: &u8) -> bool {
            self.0 == *other
        }
    }

    impl PartialEq<MessageCode> for u8 {
        fn eq(&self, other: &MessageCode) -> bool {
            *self == other.0
        }
    }

    #[inline]
    pub fn frame(buf: &mut BytesMut, payload_fn: impl FnOnce(&mut BytesMut)) {
        let base = buf.len();
        buf.put_u32(0);

        payload_fn(buf);

        let len = (buf.len() - base) as u32;
        buf[base..base + size_of::<u32>()].copy_from_slice(&len.to_be_bytes());
    }
}

pub mod backend {
    //! Logic for handling and representing Postgres backend messages.

    /// Postgres backend messages are framed by a 1 byte message code,
    /// followed by a u32 integer delineating the length of the rest of
    /// the message.
    ///
    /// The message code identifies the type of message and format of its
    /// payload.
    ///
    /// For more information, see the official Postgres docs:
    /// <https://www.postgresql.org/docs/current/protocol-message-formats.html>
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MessageCode(u8);

    impl MessageCode {
        pub const AUTHENTICATION: Self = Self(b'R');
        pub const BACKEND_KEY_DATA: Self = Self(b'K');
        pub const BIND_COMPLETE: Self = Self(b'2');
        pub const CLOSE_COMPLETE: Self = Self(b'3');
        pub const COMMAND_COMPLETE: Self = Self(b'C');
        pub const COPY_DATA: Self = Self(b'd');
        pub const COPY_DONE: Self = Self(b'c');
        pub const COPY_IN_RESPONSE: Self = Self(b'G');
        pub const COPY_OUT_RESPONSE: Self = Self(b'H');
        pub const COPY_BOTH_RESPONSE: Self = Self(b'W');
        pub const DATA_ROW: Self = Self(b'D');
        pub const EMPTY_QUERY_RESPONSE: Self = Self(b'I');
        pub const ERROR_RESPONSE: Self = Self(b'E');
        pub const FUNCTION_CALL_RESPONSE: Self = Self(b'V');
        pub const GSS_RESPONSE: Self = Self(b'p');
        pub const NEGOTIATE_PROTOCOL_VERSION: Self = Self(b'v');
        pub const NO_DATA: Self = Self(b'n');
        pub const NOTICE_RESPONSE: Self = Self(b'N');
        pub const NOTIFICATION_RESPONSE: Self = Self(b'A');
        pub const PARAMETER_DESCRIPTION: Self = Self(b't');
        pub const PARAMETER_STATUS: Self = Self(b'S');
        pub const PARSE_COMPLETE: Self = Self(b'1');
        pub const PORTAL_SUSPENDED: Self = Self(b's');
        pub const READY_FOR_QUERY: Self = Self(b'Z');
        pub const ROW_DESCRIPTION: Self = Self(b'T');
    }

    impl From<u8> for MessageCode {
        fn from(value: u8) -> Self {
            Self(value)
        }
    }

    impl From<MessageCode> for u8 {
        fn from(value: MessageCode) -> Self {
            value.0
        }
    }

    impl PartialEq<u8> for MessageCode {
        fn eq(&self, other: &u8) -> bool {
            self.0 == *other
        }
    }

    impl PartialEq<MessageCode> for u8 {
        fn eq(&self, other: &MessageCode) -> bool {
            *self == other.0
        }
    }
}
