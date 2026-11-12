//! The read side's buffer: receives bytes and frames them into messages.

use bytes::{Buf, Bytes, BytesMut};

use crate::{ProtocolError, backend::length_bounds};

/// Message code plus length field.
const HEADER_LEN: usize = 5;

const INITIAL_CAPACITY: usize = 8 * 1024;

/// The buffer grows on its own, and stays grown, only up to this size. A
/// larger message still fits, and the room made for it is released once
/// [`IDLE_READS`] reads in a row have had no use for it.
const MAX_IDLE_CAPACITY: usize = 64 * 1024;

/// How long the room made for a large message is kept. Replies of that size
/// tend to come again, and making the room anew for each can cost as much
/// as reading it.
const IDLE_READS: u8 = 16;

/// Reads are never offered less room than this, so a nearly full buffer is
/// compacted or grown rather than read into a few bytes at a time.
const MIN_SPARE: usize = 1024;

/// The largest length field PostgreSQL itself will send or accept.
pub(crate) const MAX_MESSAGE_LEN: usize = 0x3fff_ffff;

/// A place in the buffer from which consumed messages can be handed out as
/// owned bytes. It holds until the buffer is next asked for room.
#[derive(Clone, Copy)]
pub(crate) struct Mark(usize);

/// What is at the front of the buffer.
pub(crate) enum Peek {
    /// A complete message.
    Ready { code: u8, body_len: usize },
    /// Part of a message; at least this many more bytes are needed.
    Need(usize),
}

pub(crate) struct ReadBuffer {
    /// `buf[start..end]` holds the bytes received and not yet consumed. The
    /// rest of `buf` is initialized scratch space, so reads can land in
    /// `buf[end..]` without zeroing it first.
    buf: BytesMut,
    start: usize,
    end: usize,
    /// How much room the last read was offered, if it used all of it.
    saturated: Option<usize>,
    /// How many reads in a row have left no more than [`MAX_IDLE_CAPACITY`]
    /// buffered.
    idle_reads: u8,
    pub max_message_len: usize,
}

impl ReadBuffer {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::zeroed(INITIAL_CAPACITY),
            start: 0,
            end: 0,
            saturated: None,
            idle_reads: 0,
            max_message_len: MAX_MESSAGE_LEN,
        }
    }

    /// The bytes received and not yet consumed.
    #[inline]
    pub fn buffered(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    /// Looks at the message at the front of the buffer without consuming it.
    #[inline]
    pub fn peek(&self) -> Result<Peek, ProtocolError> {
        let buffered = self.buffered();
        let Some((header, body)) = buffered.split_first_chunk::<HEADER_LEN>() else {
            return Ok(Peek::Need(HEADER_LEN - buffered.len()));
        };
        let code = header[0];
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);

        // The length field counts itself but not the code.
        let body_len = match (len as usize).checked_sub(4) {
            Some(body_len) if len as usize <= self.max_message_len => body_len,
            Some(_) => return Err(self.too_large(code, len)),
            None => return Err(ProtocolError::InvalidLength { code, len }),
        };
        if body.len() >= body_len {
            return Ok(Peek::Ready { code, body_len });
        }

        let possible = length_bounds(code).is_none_or(|bounds| bounds.contains(&(len as usize)));
        if !possible {
            return Err(ProtocolError::InvalidLength { code, len });
        }
        Ok(Peek::Need(body_len - body.len()))
    }

    #[cold]
    fn too_large(&self, code: u8, len: u32) -> ProtocolError {
        ProtocolError::TooLarge {
            code,
            len,
            max: self.max_message_len,
        }
    }

    /// Consumes the message [`peek`](Self::peek) reported and returns its body.
    #[inline]
    pub fn take(&mut self, body_len: usize) -> &[u8] {
        let body = self.start + HEADER_LEN;
        self.start = body + body_len;
        &self.buf[body..self.start]
    }

    /// Like [`take`](Self::take), but hands the body out as a slice of the
    /// buffer's allocation rather than a borrow.
    #[inline]
    pub fn take_owned(&mut self, body_len: usize) -> Bytes {
        let body = Mark(self.start + HEADER_LEN);
        self.start = body.0 + body_len;
        self.take_owned_since(body)
    }

    /// Marks where the next message to be consumed begins.
    #[inline]
    pub fn mark(&self) -> Mark {
        Mark(self.start)
    }

    /// Hands out everything consumed since `mark`, headers included, as a
    /// slice of the buffer's allocation.
    #[inline]
    pub fn take_owned_since(&mut self, mark: Mark) -> Bytes {
        let mut consumed = self.buf.split_to(self.start);
        consumed.advance(mark.0);
        self.end -= self.start;
        self.start = 0;
        consumed.freeze()
    }

    /// Returns room for a read that should deliver at least `want` bytes.
    pub fn spare(&mut self, want: usize) -> &mut [u8] {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
            if self.buf.len() > MAX_IDLE_CAPACITY && self.idle_reads >= IDLE_READS {
                self.buf = BytesMut::zeroed(INITIAL_CAPACITY);
            }
        }

        // A read that used all the room it was offered means the transport had
        // more to give: offer it twice as much, up to the buffer's idle size.
        let doubled = self.saturated.map_or(0, |offered| offered * 2);
        let room = self.grow_step(want).max(doubled.min(MAX_IDLE_CAPACITY));
        if self.buf.len() - self.end < room {
            self.make_room(room);
        }
        &mut self.buf[self.end..]
    }

    /// Bounds how far one read may grow the buffer on the strength of a length
    /// field alone: a peer has to actually send a large message to make this
    /// side hold one.
    fn grow_step(&self, want: usize) -> usize {
        let buffered = self.end - self.start;
        want.min(buffered.max(MAX_IDLE_CAPACITY)).max(MIN_SPARE)
    }

    #[cold]
    fn make_room(&mut self, room: usize) {
        let buffered = self.end - self.start;
        self.buf.copy_within(self.start..self.end, 0);
        self.start = 0;
        self.end = buffered;
        if self.buf.len() - buffered >= room {
            return;
        }

        // Only the buffered bytes need to survive the move to a new allocation.
        self.buf.truncate(buffered);
        self.buf.reserve(room.max(INITIAL_CAPACITY));
        let capacity = self.buf.capacity();
        self.buf.resize(capacity, 0);
    }

    /// Records that a read placed `len` bytes in [`spare`](Self::spare).
    pub fn filled(&mut self, len: usize) {
        let offered = self.buf.len() - self.end;
        self.saturated = (len == offered).then_some(offered);
        self.end += len;
        self.idle_reads = match self.end > MAX_IDLE_CAPACITY {
            true => 0,
            false => self.idle_reads.saturating_add(1),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receive(buf: &mut ReadBuffer, bytes: &[u8]) {
        let mut bytes = bytes;
        while !bytes.is_empty() {
            let spare = buf.spare(bytes.len());
            let len = spare.len().min(bytes.len());
            spare[..len].copy_from_slice(&bytes[..len]);
            buf.filled(len);
            bytes = &bytes[len..];
        }
    }

    fn frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = vec![code];
        frame.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        frame.extend_from_slice(body);
        frame
    }

    fn ready(buf: &ReadBuffer) -> Option<(u8, usize)> {
        match buf.peek().unwrap() {
            Peek::Ready { code, body_len } => Some((code, body_len)),
            Peek::Need(_) => None,
        }
    }

    #[test]
    fn frames_messages_split_at_every_byte() {
        let mut wire = frame(b'C', b"SELECT 1\0");
        wire.extend_from_slice(&frame(b'Z', b"I"));

        for split in 0..wire.len() {
            let mut buf = ReadBuffer::new();
            receive(&mut buf, &wire[..split]);
            let mut bodies = Vec::new();
            while let Some((code, len)) = ready(&buf) {
                bodies.push((code, buf.take(len).to_vec()));
            }
            receive(&mut buf, &wire[split..]);
            while let Some((code, len)) = ready(&buf) {
                bodies.push((code, buf.take(len).to_vec()));
            }

            assert_eq!(
                bodies,
                [(b'C', b"SELECT 1\0".to_vec()), (b'Z', b"I".to_vec())]
            );
            assert!(buf.buffered().is_empty());
        }
    }

    #[test]
    fn reports_how_much_of_a_message_is_missing() {
        let mut buf = ReadBuffer::new();
        assert!(matches!(buf.peek().unwrap(), Peek::Need(5)));

        receive(&mut buf, &frame(b'D', &[0; 100])[..25]);
        assert!(matches!(buf.peek().unwrap(), Peek::Need(80)));
    }

    #[test]
    fn rejects_impossible_lengths_from_the_header() {
        let reject = |header: &[u8]| {
            let mut buf = ReadBuffer::new();
            receive(&mut buf, header);
            buf.peek().err().unwrap()
        };

        // Shorter than the length field itself.
        let err = reject(&[b'Z', 0, 0, 0, 3]);
        assert_eq!(err, ProtocolError::InvalidLength { code: b'Z', len: 3 });

        // ReadyForQuery is always five bytes.
        let err = reject(&[b'Z', 0, 0, 1, 0]);
        assert_eq!(
            err,
            ProtocolError::InvalidLength {
                code: b'Z',
                len: 256
            }
        );

        // BackendKeyData carries a key of 4 to 256 bytes.
        for len in [11, 8 + 257] {
            let mut header = vec![b'K'];
            header.extend_from_slice(&u32::to_be_bytes(len));
            assert_eq!(
                reject(&header),
                ProtocolError::InvalidLength { code: b'K', len }
            );
        }
    }

    #[test]
    fn enforces_the_message_limit_before_buffering_the_body() {
        let mut buf = ReadBuffer::new();
        buf.max_message_len = 1024;
        receive(&mut buf, &[b'D', 0, 0, 4, 1]);

        let err = buf.peek().err().unwrap();
        assert_eq!(
            err,
            ProtocolError::TooLarge {
                code: b'D',
                len: 1025,
                max: 1024
            }
        );
    }

    #[test]
    fn a_length_field_alone_cannot_force_a_large_allocation() {
        let mut buf = ReadBuffer::new();
        let mut header = vec![b'D'];
        header.extend_from_slice(&(MAX_MESSAGE_LEN as u32).to_be_bytes());
        receive(&mut buf, &header);

        let Peek::Need(missing) = buf.peek().unwrap() else {
            panic!("message is incomplete")
        };
        assert!(buf.spare(missing).len() <= 2 * MAX_IDLE_CAPACITY);
    }

    #[test]
    fn holds_a_message_larger_than_the_buffer() {
        let body = vec![7u8; 4 * MAX_IDLE_CAPACITY];
        let mut buf = ReadBuffer::new();
        receive(&mut buf, &frame(b'd', &body));

        let (code, len) = ready(&buf).unwrap();
        assert_eq!((code, buf.take(len)), (b'd', &body[..]));
    }

    #[test]
    fn keeps_the_room_for_a_large_message_while_such_messages_come() {
        let large = frame(b'd', &vec![7u8; 4 * MAX_IDLE_CAPACITY]);
        let small = frame(b'Z', b"I");
        let mut buf = ReadBuffer::new();
        // Receives and consumes one message, and reports the room offered
        // to the read after it.
        let mut room_after = |message: &[u8]| {
            receive(&mut buf, message);
            let (_, len) = ready(&buf).unwrap();
            buf.take(len);
            buf.spare(1).len()
        };

        for _ in 0..3 {
            assert!(room_after(&large) > MAX_IDLE_CAPACITY);
            for _ in 1..IDLE_READS {
                assert!(room_after(&small) > MAX_IDLE_CAPACITY);
            }
        }
        assert!(room_after(&small) <= MAX_IDLE_CAPACITY);
    }

    #[test]
    fn grows_while_reads_keep_filling_it() {
        let mut buf = ReadBuffer::new();
        let mut offered = Vec::new();
        for _ in 0..6 {
            let len = buf.spare(1).len();
            offered.push(len);
            buf.filled(len);
            buf.start = buf.end;
        }
        assert_eq!(offered, [8, 16, 32, 64, 64, 64].map(|kib| kib * 1024));
    }

    /// Owned bodies are split off the front of the buffer, so it can be left
    /// with no room at all. A read offered no room reports zero bytes, which
    /// means end-of-file.
    #[test]
    fn always_offers_room_to_read_into() {
        // 64 bytes, so that whole messages can fill a buffer exactly.
        let message = frame(b'D', &[0; 59]);
        let stream = message.repeat(4 * MAX_IDLE_CAPACITY / message.len());

        for (owned, whole_messages) in [(true, true), (true, false), (false, true), (false, false)]
        {
            let mut buf = ReadBuffer::new();
            let mut sent = 0;
            for round in 0..500 {
                // Use all the room offered, which is what asks for more.
                let room = buf.spare(1).len();
                assert!(room >= MIN_SPARE, "offered {room} bytes in round {round}");
                let len = match whole_messages {
                    true => room - room % message.len(),
                    false => room,
                };
                let from = sent % message.len();
                receive(&mut buf, &stream[from..from + len]);
                sent += len;

                while let Some((_, len)) = ready(&buf) {
                    match owned {
                        true => drop(buf.take_owned(len)),
                        false => _ = buf.take(len),
                    }
                }
            }
            assert!(buf.buffered().len() < message.len());
        }
    }

    #[test]
    fn owned_bodies_outlive_the_buffer_contents() {
        let mut wire = frame(b'D', b"first");
        wire.extend_from_slice(&frame(b'D', b"second"));
        let mut buf = ReadBuffer::new();
        receive(&mut buf, &wire);

        let (_, len) = ready(&buf).unwrap();
        let first = buf.take_owned(len);
        let (_, len) = ready(&buf).unwrap();
        let second = buf.take_owned(len);

        // Refilling the buffer must not disturb bodies already handed out.
        for _ in 0..4 {
            receive(&mut buf, &frame(b'd', &[0xff; INITIAL_CAPACITY]));
            let (_, len) = ready(&buf).unwrap();
            buf.take(len);
        }
        assert_eq!((&first[..], &second[..]), (&b"first"[..], &b"second"[..]));
    }
}
