//! The write side's buffers.

use crate::scan::FrameScanner;

/// A buffer stays grown only up to this size. A larger batch still fits, and
/// the room made for it is released once [`IDLE_BATCHES`] batches in a row
/// have had no use for it.
const MAX_IDLE_CAPACITY: usize = 64 * 1024;

/// How long the room made for a large batch is kept. Batches of that size
/// tend to come again, as the chunks of a COPY do, and making the room anew
/// for each costs more than writing it.
const IDLE_BATCHES: u8 = 16;

/// Bytes on their way to the server.
///
/// Two buffers take turns. Messages are queued in one while the other is being
/// written out, so the queue never holds bytes that are partly sent and a
/// partial write never has to shift what follows it.
#[derive(Default)]
pub(crate) struct Outbox {
    queue: Vec<u8>,
    /// `sending[sent..]` is still to be written.
    sending: Vec<u8>,
    sent: usize,
    /// How many batches in a row have fit in [`MAX_IDLE_CAPACITY`].
    idle_batches: u8,
    scanner: FrameScanner,
    /// How much of `queue` the scanner has seen.
    scanned: usize,
}

impl Outbox {
    /// The messages queued and not yet picked up for sending.
    pub fn queue(&mut self) -> &mut Vec<u8> {
        &mut self.queue
    }

    /// The number of bytes not yet written.
    pub fn len(&self) -> usize {
        self.queue.len() + self.sending.len() - self.sent
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Queues what `write` appends, and calls `message` with its code if it is
    /// one whole message. What an encoder appends is, and is known that way
    /// without being scanned for.
    #[inline]
    pub fn encode<R>(
        &mut self,
        write: impl FnOnce(&mut Vec<u8>) -> R,
        mut message: impl FnMut(u8),
    ) -> R {
        let start = self.queue.len();
        let result = write(&mut self.queue);
        if let Some(code) = self.encoded_at(start) {
            message(code);
            self.scanned = self.queue.len();
        }
        result
    }

    /// The code of the message the queue holds from `start` on, if that is
    /// exactly one message and the scanner is level with its start. Anything
    /// else is the scanner's to follow: bytes appended to the queue directly,
    /// and whatever comes after them until it has caught up.
    #[inline]
    fn encoded_at(&self, start: usize) -> Option<u8> {
        let level = self.scanned == start && self.scanner.is_between_messages();
        let (&[code, a, b, c, d], body) = self.queue.get(start..)?.split_first_chunk()?;
        let whole = u32::from_be_bytes([a, b, c, d]) as usize == body.len() + 4;
        (level && whole).then_some(code)
    }

    /// Calls `message` with the code of each message queued since the last
    /// call.
    pub fn scan(&mut self, message: impl FnMut(u8)) {
        // The queue only grows at its end, short of a caller cutting it back.
        let unscanned = self.queue.get(self.scanned..).unwrap_or_default();
        self.scanner.feed(unscanned, message);
        self.scanned = self.queue.len();
    }

    /// The bytes to write next, which is empty once everything is written.
    /// Messages newly picked up from the queue are reported as by
    /// [`scan`](Self::scan).
    pub fn pending(&mut self, message: impl FnMut(u8)) -> &[u8] {
        if self.sent == self.sending.len() {
            self.scan(message);
            self.sending.clear();
            std::mem::swap(&mut self.sending, &mut self.queue);
            self.sent = 0;
            self.scanned = 0;

            self.idle_batches = match self.sending.len() {
                0 => self.idle_batches,
                1..=MAX_IDLE_CAPACITY => self.idle_batches.saturating_add(1),
                _ => 0,
            };
            if self.queue.capacity() > MAX_IDLE_CAPACITY && self.idle_batches >= IDLE_BATCHES {
                self.queue = Vec::new();
            }
        }
        &self.sending[self.sent..]
    }

    /// Records that the first `written` bytes of [`pending`](Self::pending)
    /// have been written.
    pub fn advance(&mut self, written: usize) {
        self.sent += written;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Frontend;

    /// Writes everything pending, at most `step` bytes at a time.
    fn drain(outbox: &mut Outbox, step: usize, codes: &mut Vec<u8>) -> Vec<u8> {
        let mut written = Vec::new();
        loop {
            let pending = outbox.pending(|code| codes.push(code));
            let len = pending.len().min(step);
            if len == 0 {
                return written;
            }
            written.extend_from_slice(&pending[..len]);
            outbox.advance(len);
        }
    }

    #[test]
    fn writes_everything_queued_in_order_and_reports_it_once() {
        let mut outbox = Outbox::default();
        let (mut expected, mut codes) = (Vec::new(), Vec::new());

        outbox.queue().query("SELECT 1").sync();
        expected.query("SELECT 1").sync();

        // Queueing while a write is in progress keeps the order.
        let pending = outbox.pending(|code| codes.push(code)).len();
        outbox.advance(pending / 2);
        outbox.queue().copy_done().terminate();
        expected.copy_done().terminate();
        assert_eq!(outbox.len(), expected.len() - pending / 2);

        let mut written = expected[..pending / 2].to_vec();
        written.extend(drain(&mut outbox, 3, &mut codes));
        assert_eq!(written, expected);
        assert_eq!(codes, b"QScX");
        assert!(outbox.is_empty());
    }

    #[test]
    fn scanning_ahead_does_not_report_a_message_twice() {
        let mut outbox = Outbox::default();
        let mut codes = Vec::new();

        outbox.queue().copy_data(b"row");
        outbox.scan(|code| codes.push(code));
        outbox.queue().copy_done();
        outbox.scan(|code| codes.push(code));
        outbox.scan(|code| codes.push(code));
        drain(&mut outbox, usize::MAX, &mut codes);

        assert_eq!(codes, b"dc");
    }

    #[test]
    fn encoded_and_appended_messages_are_each_reported_once() {
        let mut outbox = Outbox::default();
        let mut codes = Vec::new();

        outbox.encode(|buf| _ = buf.query("COPY"), |code| codes.push(code));
        assert_eq!(codes, b"Q");

        // A message appended directly is the scanner's to find, and so is
        // one encoded behind it.
        outbox.queue().copy_data(b"row");
        outbox.encode(|buf| _ = buf.copy_done(), |code| codes.push(code));
        assert_eq!(codes, b"Q");
        outbox.scan(|code| codes.push(code));
        assert_eq!(codes, b"Qdc");

        // So is one encoded into the middle of an appended one. A CopyData
        // of five bytes is appended without them, and a Sync becomes them.
        outbox.queue().extend_from_slice(b"d\0\0\0\x09");
        outbox.scan(|code| codes.push(code));
        outbox.encode(|buf| _ = buf.sync(), |code| codes.push(code));
        drain(&mut outbox, 3, &mut codes);
        assert_eq!(codes, b"Qdcd");

        // An encoder that fails queues nothing, and one that queues more than
        // a message leaves them to the scanner.
        outbox.encode(|_| (), |code| codes.push(code));
        outbox.encode(|buf| _ = buf.copy_done().sync(), |code| codes.push(code));
        outbox.encode(|buf| _ = buf.terminate(), |code| codes.push(code));
        assert_eq!(codes, b"Qdcd");
        drain(&mut outbox, usize::MAX, &mut codes);
        assert_eq!(codes, b"QdcdcSX");
    }

    #[test]
    fn keeps_the_room_for_a_large_batch_while_such_batches_come() {
        let mut outbox = Outbox::default();
        // Queues and writes one batch, and reports the room left behind.
        let mut room_after = |batch: &[u8]| {
            outbox.queue().copy_data(batch);
            drain(&mut outbox, usize::MAX, &mut Vec::new());
            outbox.queue.capacity().max(outbox.sending.capacity())
        };
        let large = vec![0; 4 * MAX_IDLE_CAPACITY];

        for _ in 0..3 {
            assert!(room_after(&large) > MAX_IDLE_CAPACITY);
            for _ in 1..IDLE_BATCHES {
                assert!(room_after(b"row") > MAX_IDLE_CAPACITY);
            }
        }
        assert!(room_after(b"row") <= MAX_IDLE_CAPACITY);
    }
}
