//! The write side's buffers.

use crate::scan::FrameScanner;

/// A buffer is released once it has been emptied at above this capacity, so
/// one large message does not pin its memory for good.
const MAX_IDLE_CAPACITY: usize = 64 * 1024;

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
            if self.sending.capacity() > MAX_IDLE_CAPACITY {
                self.sending = Vec::new();
            }
            std::mem::swap(&mut self.sending, &mut self.queue);
            self.sent = 0;
            self.scanned = 0;
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
    fn releases_the_memory_of_a_large_message() {
        let mut outbox = Outbox::default();
        outbox.queue().copy_data(&vec![0; 4 * MAX_IDLE_CAPACITY]);
        drain(&mut outbox, usize::MAX, &mut Vec::new());

        // Both buffers have had their turn at holding it.
        outbox.queue().sync();
        drain(&mut outbox, usize::MAX, &mut Vec::new());
        assert!(outbox.queue.capacity() <= MAX_IDLE_CAPACITY);
        assert!(outbox.sending.capacity() <= MAX_IDLE_CAPACITY);
    }
}
