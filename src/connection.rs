//! A connection to a server.

use std::{
    future::poll_fn,
    io,
    task::{Context, Poll, ready},
};

use crate::{
    Error, Frontend, ProtocolError, Response, Result, Transport,
    backend::{AsyncMessage, ErrorResponse, Frame, Frames, Message, TransactionStatus, code},
    buffer::{Peek, ReadBuffer},
    outbox::Outbox,
};

/// How many bytes of asynchronous messages are kept for
/// [`Connection::async_messages`] before new ones are dropped.
const MAX_ASYNC_BACKLOG: usize = 256 * 1024;

/// A connection to a server: a stream, plus the buffers and bookkeeping needed
/// to speak the protocol over it.
///
/// # Sending
///
/// [`Frontend`] methods queue messages; nothing reaches the server until the
/// queue is written out. That happens in [`flush`](Self::flush), and also
/// whenever the connection waits for the server, so queued messages are never
/// stranded behind a read:
///
/// ```no_run
/// # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
/// use pg_stream::Frontend;
///
/// conn.query("SELECT 1");
/// let status = conn.response().finish().await?; // sends the query, then reads
/// # Ok(())
/// # }
/// ```
///
/// # Receiving
///
/// - [`response`](Self::response) reads one cycle of replies with the
///   protocol's rules applied. This is the place to start.
/// - [`recv`](Self::recv) returns every message exactly as the server sent it.
/// - [`recv_frame`](Self::recv_frame) does the same with an owned message, for
///   handing to another task.
/// - [`recv_frames`](Self::recv_frames) does the same with every message that
///   has arrived, in one owned piece.
///
/// # Pipelining
///
/// Queue as many cycles as you like before reading any replies. Reads keep
/// writing and writes keep reading, so neither side can stall the other with a
/// full socket buffer however large the pipeline. Replies the server sends
/// while a large pipeline is still being written are held in memory.
///
/// That takes a transport which can wait on both directions at once.
/// [`Blocking`](crate::Blocking) cannot, and says what to do instead.
///
/// # Cancel safety
///
/// Every future returned by a connection can be dropped at any point. No bytes
/// are lost or sent twice, and the next call picks up where it left off.
pub struct Connection<S> {
    stream: S,
    read: ReadBuffer,
    /// The stream reported end-of-file while output was being flushed.
    read_closed: bool,
    outbox: Outbox,
    /// Bytes have been written to the stream since it was last flushed.
    unflushed: bool,
    state: State,
    backlog: AsyncBacklog,
}

struct State {
    status: TransactionStatus,
    /// A [`Response`] has been handed out and its ReadyForQuery has not been
    /// read yet.
    cycle_open: bool,
    /// Cycles whose [`Response`] was dropped early. Their remaining replies
    /// are discarded before anything else is read.
    abandoned: u32,
    /// The server is waiting for COPY data, and the message that ends the
    /// COPY has not been queued.
    copy_in: bool,
    /// CopyDone and CopyFail messages queued while no COPY was known to be
    /// waiting for one. A pipeline can end a COPY before its CopyInResponse
    /// has been read, so each of these answers one COPY still to come.
    ///
    /// The server ignores a CopyDone or CopyFail that reaches it outside a
    /// COPY, typically after the COPY failed. That one is counted here all the
    /// same, and makes the next COPY look ended before it is. Erring that way
    /// only costs the diagnosis of a COPY left waiting; the opposite error
    /// would refuse pipelines that are correct.
    copy_ends_queued: u32,
    desynchronized: bool,
}

impl State {
    #[inline]
    fn observe(&mut self, code: u8, body: &[u8]) {
        match code {
            code::READY_FOR_QUERY => {
                self.status = TransactionStatus::try_parse(body).unwrap_or(self.status);
                self.copy_in = false;
            }
            code::COPY_IN_RESPONSE | code::COPY_BOTH_RESPONSE => {
                match self.copy_ends_queued.checked_sub(1) {
                    Some(remaining) => self.copy_ends_queued = remaining,
                    None => self.copy_in = true,
                }
            }
            code::COMMAND_COMPLETE | code::ERROR_RESPONSE => self.copy_in = false,
            _ => {}
        }
    }

    /// Notes a message that has been queued for sending.
    fn queued(&mut self, code: u8) {
        if matches!(code, b'c' | b'f') && !std::mem::take(&mut self.copy_in) {
            self.copy_ends_queued += 1;
        }
    }
}

impl<S> Connection<S> {
    /// Wraps a stream that is ready for the startup packet, or on which
    /// startup has already been completed by other means.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            read: ReadBuffer::new(),
            read_closed: false,
            outbox: Outbox::default(),
            unflushed: false,
            state: State {
                status: TransactionStatus::Idle,
                cycle_open: false,
                abandoned: 0,
                copy_in: false,
                copy_ends_queued: 0,
                desynchronized: false,
            },
            backlog: AsyncBacklog::default(),
        }
    }

    /// Limits the size of the messages this connection will accept, to bound
    /// the memory a server can make it use. The default is the protocol's own
    /// maximum of 1 GiB.
    pub fn set_max_message_len(&mut self, max: usize) {
        self.read.max_message_len = max;
    }

    /// The transaction state as of the last ReadyForQuery.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.state.status
    }

    /// The NoticeResponse, NotificationResponse and ParameterStatus messages
    /// that arrived during [`Response`]s since this was last called.
    ///
    /// The server may send these at any time, so responses set them aside
    /// rather than treat them as replies. Up to 256 KiB are kept; call this
    /// regularly if you care about them. [`recv`](Self::recv) and
    /// [`recv_frame`](Self::recv_frame) return them in place instead.
    pub fn async_messages(&mut self) -> impl Iterator<Item = AsyncMessage<'_>> {
        self.backlog.drain()
    }

    /// The queue of messages to send. Whole messages encoded elsewhere can
    /// be appended to it directly.
    ///
    /// A write takes what it sends out of the queue, so an empty queue does
    /// not mean that everything has been written. [`unsent`](Self::unsent)
    /// says how much has not.
    pub fn buf(&mut self) -> &mut Vec<u8> {
        self.outbox.queue()
    }

    /// The number of bytes queued and not yet written to the stream.
    pub fn unsent(&self) -> usize {
        self.outbox.len()
    }

    /// The underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// The underlying stream. Reading or writing it directly bypasses the
    /// connection's buffers.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Returns the stream, discarding anything buffered.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Brings the connection's view of what has been queued up to date.
    fn scan_queued(&mut self) {
        self.outbox.scan(|code| self.state.queued(code));
    }

    /// Whether the server is waiting for COPY data that nothing queued so far
    /// will bring to an end.
    fn awaits_copy_data(&mut self) -> bool {
        self.scan_queued();
        self.state.copy_in
    }

    /// Fails if the replies up to the next ReadyForQuery cannot be skipped,
    /// because there will not be one.
    pub(crate) fn check_skippable(&mut self) -> Result<()> {
        if self.state.desynchronized {
            return Err(ProtocolError::Desynchronized.into());
        }
        if self.awaits_copy_data() {
            return Err(Error::unexpected(code::COPY_IN_RESPONSE, "ReadyForQuery"));
        }
        Ok(())
    }

    /// Whether reading the message with this code would leave the server
    /// waiting for COPY data that nothing queued so far will bring to an end.
    pub(crate) fn starts_open_copy(&mut self, code: u8) -> bool {
        let starts_copy = matches!(code, code::COPY_IN_RESPONSE | code::COPY_BOTH_RESPONSE);
        starts_copy && {
            self.scan_queued();
            self.state.copy_ends_queued == 0
        }
    }

    /// The message at the front of the read buffer, if a whole one is there
    /// and nothing has to happen before it is handed out.
    ///
    /// Messages usually arrive many to a read, so this is the common case. It
    /// is answered without the machinery of [`poll_next`](Self::poll_next).
    #[inline]
    fn buffered_message(&self) -> Option<(u8, usize)> {
        if self.state.abandoned > 0 || self.state.desynchronized {
            return None;
        }
        match self.read.peek() {
            Ok(Peek::Ready { code, body_len }) => Some((code, body_len)),
            Ok(Peek::Need(_)) | Err(_) => None,
        }
    }

    /// Like [`buffered_message`](Self::buffered_message), for a reply.
    #[inline]
    pub(crate) fn buffered_reply(&self) -> Option<(u8, usize)> {
        let message = self.buffered_message();
        message.filter(|&(code, _)| !code::is_async(code))
    }

    /// Consumes the message [`poll_peek`](Self::poll_peek) reported, on
    /// behalf of whoever is reading the connection now.
    #[inline]
    pub(crate) fn take(&mut self, code: u8, body_len: usize) -> &[u8] {
        self.before_consuming(code);
        let body = self.read.take(body_len);
        self.state.observe(code, body);
        body
    }

    /// What reading a message changes before its body is looked at.
    #[inline]
    fn before_consuming(&mut self, code: u8) {
        match code {
            // Abandoned cycles are discarded before anything else is read, so
            // while any remain this ReadyForQuery ends one of them. It says
            // nothing about a response opened since.
            code::READY_FOR_QUERY => match self.state.abandoned.checked_sub(1) {
                Some(remaining) => self.state.abandoned = remaining,
                None => self.state.cycle_open = false,
            },
            code::COPY_IN_RESPONSE | code::COPY_BOTH_RESPONSE => self.scan_queued(),
            _ => {}
        }
    }

    /// Consumes an ErrorResponse and turns it into the error to return.
    pub(crate) fn take_error(&mut self, body_len: usize) -> Error {
        const CODE: u8 = code::ERROR_RESPONSE;
        match ErrorResponse::try_parse(self.take(CODE, body_len)) {
            Some(fields) => Error::Server(fields.to_error()),
            None => ProtocolError::Malformed { code: CODE }.into(),
        }
    }

    /// Consumes an asynchronous message into the backlog.
    fn set_aside(&mut self, code: u8, body_len: usize) -> Result<()> {
        let body = self.read.take(body_len);
        Ok(self.backlog.push(code, body)?)
    }

    /// Called when the connection is read directly. A [`Response`] borrows
    /// the connection, so if its cycle is still open here, it was dropped
    /// before the cycle ended and what is left of the cycle must be skipped.
    fn abandon_open_cycle(&mut self) {
        if std::mem::take(&mut self.state.cycle_open) {
            self.state.abandoned += 1;
            // Nothing can be skipped while the server waits on us for COPY
            // data, and nothing here knows how to end that COPY on the
            // caller's behalf.
            self.state.desynchronized |= self.awaits_copy_data();
        }
    }
}

impl<S: Transport> Connection<S> {
    /// Sends everything queued and waits until the stream has accepted it.
    ///
    /// Only needed when no read follows, since reads send too.
    pub async fn flush(&mut self) -> Result<()> {
        poll_fn(|cx| self.poll_flush(cx)).await
    }

    /// Receives the next message, whatever it is.
    ///
    /// This is the unfiltered view of the connection: asynchronous messages
    /// arrive in place, errors arrive as [`Message::ErrorResponse`], and
    /// knowing which reply answers which request is up to the caller.
    pub async fn recv(&mut self) -> Result<Message<'_>> {
        self.abandon_open_cycle();
        let (code, body_len) = match self.buffered_message() {
            Some(message) => message,
            None => self.wait_for_message().await?,
        };
        Ok(Message::parse(code, self.take(code, body_len))?)
    }

    /// Like [`recv`](Self::recv), but returns an owned [`Frame`] that can be
    /// sent to another task. The body shares the read buffer's allocation
    /// instead of being copied out of it.
    pub async fn recv_frame(&mut self) -> Result<Frame> {
        self.abandon_open_cycle();
        let (code, body_len) = match self.buffered_message() {
            Some(message) => message,
            None => self.wait_for_message().await?,
        };
        self.before_consuming(code);
        let body = self.read.take_owned(body_len);
        self.state.observe(code, &body);
        Ok(Frame { code, body })
    }

    /// Like [`recv_frame`](Self::recv_frame), but returns every message that
    /// has arrived, up to and including the next ReadyForQuery, as one owned
    /// piece of the read buffer.
    ///
    /// ```no_run
    /// # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
    /// use pg_stream::{Frontend, Message};
    ///
    /// conn.query("SELECT generate_series(1, 1000)");
    /// loop {
    ///     let frames = conn.recv_frames().await?;
    ///     for message in frames.messages() {
    ///         if let Message::DataRow(row) = message? {
    ///             println!("{row:?}");
    ///         }
    ///     }
    ///     if frames.ends_cycle() {
    ///         break;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv_frames(&mut self) -> Result<Frames> {
        self.abandon_open_cycle();
        let (mut code, mut body_len) = match self.buffered_message() {
            Some(message) => message,
            None => self.wait_for_message().await?,
        };
        let mark = self.read.mark();
        let ends_cycle = loop {
            self.take(code, body_len);
            if code == code::READY_FOR_QUERY {
                break true;
            }
            match self.buffered_message() {
                Some(next) => (code, body_len) = next,
                None => break false,
            }
        };
        Ok(Frames::new(self.read.take_owned_since(mark), ends_cycle))
    }

    /// Waits for the next message. Callers try
    /// [`buffered_message`](Self::buffered_message) first, in their own
    /// bodies: a future awaited there is not reliably inlined, and this one
    /// is only needed when the message has yet to arrive.
    async fn wait_for_message(&mut self) -> Result<(u8, usize)> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Like [`wait_for_message`](Self::wait_for_message), for a reply.
    pub(crate) async fn wait_for_reply(&mut self) -> Result<(u8, usize)> {
        poll_fn(|cx| self.poll_reply(cx)).await
    }

    /// Reads the replies to one cycle: everything up to and including the
    /// next ReadyForQuery.
    pub fn response(&mut self) -> Response<'_, S> {
        self.abandon_open_cycle();
        self.state.cycle_open = true;
        Response::new(self)
    }

    /// Writes queued output for as long as the stream accepts it. Ready once
    /// all of it has been written and flushed.
    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let pending = self.outbox.pending(|code| self.state.queued(code));
            if pending.is_empty() {
                break;
            }
            match ready!(self.stream.poll_write(cx, pending))? {
                0 => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                written => self.outbox.advance(written),
            }
            self.unflushed = true;
        }

        if self.unflushed {
            ready!(self.stream.poll_flush(cx))?;
            self.unflushed = false;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while self.poll_send(cx)?.is_pending() {
            // The stream is not taking our output. If that is because the
            // server is itself stuck sending replies we have not read, taking
            // them off the wire is the only way either side moves again.
            if self.read_closed {
                return Poll::Pending;
            }
            let spare = self.read.spare(1);
            match ready!(self.stream.poll_read(cx, spare))? {
                0 => self.read_closed = true,
                read => self.read.filled(read),
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Makes a whole message available at the front of the read buffer and
    /// reports its code and body length, without consuming it.
    fn poll_peek(&mut self, cx: &mut Context<'_>) -> Poll<Result<(u8, usize)>> {
        loop {
            let missing = match self.read.peek()? {
                Peek::Ready { code, body_len } => return Poll::Ready(Ok((code, body_len))),
                Peek::Need(missing) => missing,
            };
            if self.read_closed {
                return Poll::Ready(Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()));
            }

            // Whatever is queued has to go out before the server can answer.
            // A failed write is not reported here: a server that hangs up has
            // usually said why first, and that message is still to be read.
            if !self.outbox.is_empty() || self.unflushed {
                let _ = self.poll_send(cx);
            }

            let spare = self.read.spare(missing);
            match ready!(self.stream.poll_read(cx, spare))? {
                0 => self.read_closed = true,
                read => self.read.filled(read),
            }
        }
    }

    /// [`poll_peek`](Self::poll_peek), once the replies of abandoned cycles
    /// have been discarded.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<(u8, usize)>> {
        if self.state.desynchronized {
            return Poll::Ready(Err(ProtocolError::Desynchronized.into()));
        }
        while self.state.abandoned > 0 {
            let (code, body_len) = ready!(self.poll_peek(cx))?;
            if self.starts_open_copy(code) {
                self.state.desynchronized = true;
                return Poll::Ready(Err(ProtocolError::Desynchronized.into()));
            }
            self.discard(code, body_len)?;
        }
        self.poll_peek(cx)
    }

    /// Like [`poll_next`](Self::poll_next), but sets asynchronous messages
    /// aside so that only replies are reported.
    fn poll_reply(&mut self, cx: &mut Context<'_>) -> Poll<Result<(u8, usize)>> {
        loop {
            let (code, body_len) = ready!(self.poll_next(cx))?;
            if !code::is_async(code) {
                return Poll::Ready(Ok((code, body_len)));
            }
            self.set_aside(code, body_len)?;
        }
    }

    /// Consumes a message of an abandoned cycle.
    fn discard(&mut self, code: u8, body_len: usize) -> Result<()> {
        match code::is_async(code) {
            true => self.set_aside(code, body_len)?,
            false => _ = self.take(code, body_len),
        }
        Ok(())
    }
}

impl<S> Frontend for Connection<S> {
    type Buf = Vec<u8>;

    fn with_buf<R>(&mut self, write: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        write(self.buf())
    }
}

impl<S: std::fmt::Debug> std::fmt::Debug for Connection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("Connection");
        debug.field("stream", &self.stream);
        debug.field("unsent", &self.unsent());
        debug.field("unread", &self.read.buffered().len());
        debug.field("transaction_status", &self.state.status);
        debug.finish_non_exhaustive()
    }
}

/// Asynchronous messages set aside while reading replies, stored as the raw
/// frames they arrived in.
#[derive(Default)]
struct AsyncBacklog {
    frames: Vec<u8>,
    /// `frames` has been handed out by [`drain`](Self::drain) and is stale.
    drained: bool,
}

impl AsyncBacklog {
    fn push(&mut self, code: u8, body: &[u8]) -> Result<(), ProtocolError> {
        AsyncMessage::try_parse(code, body).ok_or(ProtocolError::Malformed { code })?;
        if std::mem::take(&mut self.drained) {
            self.frames.clear();
        }
        if self.frames.len() + body.len() <= MAX_ASYNC_BACKLOG {
            let len = body.len() as u32;
            self.frames.push(code);
            self.frames.extend_from_slice(&len.to_be_bytes());
            self.frames.extend_from_slice(body);
        }
        Ok(())
    }

    fn drain(&mut self) -> impl Iterator<Item = AsyncMessage<'_>> {
        if std::mem::replace(&mut self.drained, true) {
            self.frames.clear();
        }
        let mut frames = &self.frames[..];
        std::iter::from_fn(move || {
            let (&[code, len @ ..], rest) = frames.split_first_chunk::<5>()?;
            let (body, rest) = rest.split_at_checked(u32::from_be_bytes(len) as usize)?;
            frames = rest;
            AsyncMessage::try_parse(code, body)
        })
    }
}
