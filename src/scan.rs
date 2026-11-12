//! Following the messages in the outgoing byte stream.

/// Walks a stream of client-to-server bytes and reports the code of each
/// message in it, however the stream is cut into pieces.
///
/// The connection learns what has been queued this way, rather than from the
/// encoders, because messages can also be appended already encoded.
#[derive(Default)]
pub(crate) struct FrameScanner {
    /// The start of a header that the stream was cut in the middle of.
    split: [u8; 5],
    /// How much of that header has been collected.
    have: usize,
    /// How much of the current message's body is still to come.
    skip: usize,
}

/// What the header of a message says about it.
struct Header {
    /// Zero for a packet that has none.
    code: u8,
    body_len: usize,
}

impl Header {
    /// Reads the header that `bytes` begins with, if all of it is there.
    #[inline]
    fn parse(bytes: &[u8]) -> Option<Self> {
        // The packets that open a connection (startup, TLS and cancel
        // requests) have no code. They begin with their length, whose first
        // byte is always zero, which no code is.
        let (code, len) = match *bytes {
            [0, a, b, c, ..] => (0, [0, a, b, c]),
            [code, a, b, c, d, ..] => (code, [a, b, c, d]),
            _ => return None,
        };
        let body_len = (u32::from_be_bytes(len) as usize).saturating_sub(4);
        Some(Self { code, body_len })
    }

    /// The length of the header itself.
    #[inline]
    fn len(&self) -> usize {
        match self.code {
            0 => 4,
            _ => 5,
        }
    }
}

impl FrameScanner {
    /// Scans the next bytes of the stream, calling `message` with the code of
    /// each message that begins in them. Packets without a code report zero.
    pub fn feed(&mut self, mut bytes: &[u8], mut message: impl FnMut(u8)) {
        while !bytes.is_empty() {
            if self.skip > 0 {
                let skipped = self.skip.min(bytes.len());
                self.skip -= skipped;
                bytes = &bytes[skipped..];
                continue;
            }

            let header = match (self.have, Header::parse(bytes)) {
                // A header nearly always arrives whole, and is read where it
                // lies.
                (0, Some(header)) => {
                    bytes = &bytes[header.len()..];
                    header
                }
                // One that the stream was cut in the middle of is put
                // together a byte at a time.
                _ => {
                    self.split[self.have] = bytes[0];
                    self.have += 1;
                    bytes = &bytes[1..];
                    let Some(header) = Header::parse(&self.split[..self.have]) else {
                        continue;
                    };
                    self.have = 0;
                    header
                }
            };
            self.skip = header.body_len;
            message(header.code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Format, Frontend, ProtocolVersion, params};

    fn pipeline() -> (Vec<u8>, Vec<u8>) {
        let mut buf = Vec::new();
        buf.startup_message(ProtocolVersion::V3_0, [("user", "postgres")])
            .password_message("secret")
            .query("COPY t FROM STDIN")
            .copy_data(&[b'c'; 300])
            .copy_data(b"")
            .copy_done()
            .parse("", "SELECT $1", &[])
            .bind("", "", params!["c", "f"], Format::Text)
            .execute("", 0)
            .copy_fail("f")
            .sync()
            .ssl_request()
            .cancel_request(7, &[b'f'; 32])
            .terminate();
        (buf, b"\0pQddcPBEfS\0\0X".to_vec())
    }

    fn scan(pieces: &[&[u8]]) -> Vec<u8> {
        let mut scanner = FrameScanner::default();
        let mut codes = Vec::new();
        for piece in pieces {
            scanner.feed(piece, |code| codes.push(code));
        }
        codes
    }

    #[test]
    fn reports_every_message_once() {
        let (buf, codes) = pipeline();
        assert_eq!(scan(&[&buf]), codes);
    }

    #[test]
    fn follows_messages_across_any_split() {
        let (buf, codes) = pipeline();
        for at in 0..=buf.len() {
            assert_eq!(scan(&[&buf[..at], &buf[at..]]), codes, "split at {at}");
        }

        let bytes: Vec<&[u8]> = buf.chunks(1).collect();
        assert_eq!(scan(&bytes), codes);
    }
}
