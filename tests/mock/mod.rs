//! An in-memory server whose behaviour a test controls byte by byte.
#![allow(dead_code)]

use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use pg_stream::{
    Transport,
    backend::{AsyncMessage, RowDescription},
};

/// The test's side of a connection.
#[derive(Clone)]
pub struct Server(Rc<RefCell<State>>);

/// The client's side, to build a `Connection` on.
pub struct Stream(Rc<RefCell<State>>);

struct State {
    to_client: VecDeque<u8>,
    from_client: Vec<u8>,
    /// How much of `from_client` the script has already answered.
    answered: usize,
    script: VecDeque<(u8, Reply)>,
    max_read: usize,
    max_write: usize,
    /// How many more bytes the client is given before reads block.
    read_room: usize,
    /// How many more bytes the server takes before writes block.
    write_room: usize,
    /// How many unread bytes the server lets pile up before it stops taking
    /// writes.
    backlog: usize,
    write_error: Option<io::ErrorKind>,
    closed: bool,
    reads: usize,
    flushes: usize,
}

/// Computes the reply to a client message from its body.
type Reply = Box<dyn FnMut(&[u8]) -> Vec<u8>>;

pub fn pair() -> (Server, Stream) {
    let state = Rc::new(RefCell::new(State {
        to_client: VecDeque::new(),
        from_client: Vec::new(),
        answered: 0,
        script: VecDeque::new(),
        max_read: usize::MAX,
        max_write: usize::MAX,
        read_room: usize::MAX,
        write_room: usize::MAX,
        backlog: usize::MAX,
        write_error: None,
        closed: false,
        reads: 0,
        flushes: 0,
    }));
    (Server(state.clone()), Stream(state))
}

impl Server {
    /// Makes bytes available for the client to read.
    pub fn send(&self, bytes: &[u8]) -> &Self {
        self.0.borrow_mut().to_client.extend(bytes);
        self
    }

    /// Answers the client's next message, which must have the code `expect`
    /// (zero for a packet without one), with `reply`.
    pub fn reply(&self, expect: u8, reply: &[u8]) -> &Self {
        let reply = reply.to_vec();
        self.reply_with(expect, move |_| reply.clone())
    }

    /// Like [`reply`](Self::reply), with a reply computed from the body of
    /// the client's message.
    pub fn reply_with(&self, expect: u8, reply: impl FnMut(&[u8]) -> Vec<u8> + 'static) -> &Self {
        let step: (u8, Reply) = (expect, Box::new(reply));
        self.0.borrow_mut().script.push_back(step);
        self
    }

    /// Closes the connection once the client has read everything sent.
    pub fn close(&self) -> &Self {
        self.0.borrow_mut().closed = true;
        self
    }

    /// Delivers at most `max` bytes per read.
    pub fn max_read(&self, max: usize) -> &Self {
        self.0.borrow_mut().max_read = max;
        self
    }

    /// Takes at most `max` bytes per write.
    pub fn max_write(&self, max: usize) -> &Self {
        self.0.borrow_mut().max_write = max;
        self
    }

    /// Gives the client `room` more bytes in total, then blocks reads.
    pub fn read_room(&self, room: usize) -> &Self {
        self.0.borrow_mut().read_room = room;
        self
    }

    /// Takes `room` more bytes in total, then blocks writes.
    pub fn write_room(&self, room: usize) -> &Self {
        self.0.borrow_mut().write_room = room;
        self
    }

    /// Stops taking writes while `backlog` or more bytes are waiting to be
    /// read, as a server does that cannot send its replies.
    pub fn backlog(&self, backlog: usize) -> &Self {
        self.0.borrow_mut().backlog = backlog;
        self
    }

    /// Fails every write.
    pub fn fail_writes(&self, kind: io::ErrorKind) -> &Self {
        self.0.borrow_mut().write_error = Some(kind);
        self
    }

    /// Everything the client has written since this was last called.
    pub fn received(&self) -> Vec<u8> {
        let mut state = self.0.borrow_mut();
        state.answered = 0;
        std::mem::take(&mut state.from_client)
    }

    /// How many bytes the client has not read yet.
    pub fn unread(&self) -> usize {
        self.0.borrow().to_client.len()
    }

    pub fn reads(&self) -> usize {
        self.0.borrow().reads
    }

    pub fn flushes(&self) -> usize {
        self.0.borrow().flushes
    }

    /// Whether every scripted reply has been given.
    pub fn script_finished(&self) -> bool {
        self.0.borrow().script.is_empty()
    }
}

impl State {
    /// Gives the scripted replies to the messages the client has completed.
    fn run_script(&mut self) {
        while let Some((code, len)) = client_message(&self.from_client[self.answered..]) {
            let Some((expect, mut reply)) = self.script.pop_front() else {
                return;
            };
            assert_eq!(code as char, expect as char, "unexpected client message");
            let header = if code == 0 { 4 } else { 5 };
            let body = &self.from_client[self.answered + header..self.answered + len];
            self.to_client.extend(reply(body));
            self.answered += len;
        }
    }
}

/// The code and total length of the message at the front of `bytes`, if all
/// of it is there. Packets without a code report zero.
fn client_message(bytes: &[u8]) -> Option<(u8, usize)> {
    let (code, header) = match *bytes.first()? {
        0 => (0, 0),
        code => (code, 1),
    };
    let len = bytes.get(header..header + 4)?;
    let len = header + u32::from_be_bytes(len.try_into().unwrap()) as usize;
    (bytes.len() >= len).then_some((code, len))
}

/// Splits what a client wrote into `(code, body)` messages.
pub fn client_messages(mut bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    while let Some((code, len)) = client_message(bytes) {
        let header = if code == 0 { 4 } else { 5 };
        messages.push((code, bytes[header..len].to_vec()));
        bytes = &bytes[len..];
    }
    assert!(bytes.is_empty(), "the client wrote a partial message");
    messages
}

impl Transport for Stream {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut state = self.0.borrow_mut();
        let available = state.to_client.len().min(state.read_room);
        let len = buf.len().min(state.max_read).min(available);
        if len == 0 {
            assert!(!buf.is_empty(), "a read must be given room");
            let at_end = state.closed && state.to_client.is_empty();
            return if at_end {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            };
        }
        state.read_room -= len;
        state.reads += 1;
        for (to, from) in buf.iter_mut().zip(state.to_client.drain(..len)) {
            *to = from;
        }
        Poll::Ready(Ok(len))
    }

    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let mut state = self.0.borrow_mut();
        if let Some(kind) = state.write_error {
            return Poll::Ready(Err(kind.into()));
        }
        let len = buf.len().min(state.max_write).min(state.write_room);
        if len == 0 || state.to_client.len() >= state.backlog {
            return Poll::Pending;
        }
        state.write_room -= len;
        state.from_client.extend_from_slice(&buf[..len]);
        state.run_script();
        Poll::Ready(Ok(len))
    }

    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.borrow_mut().flushes += 1;
        Poll::Ready(Ok(()))
    }
}

/// A small deterministic generator: enough to vary a test, and reproducible.
pub struct Random(pub u64);

impl Random {
    /// A number below `bound`.
    pub fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

/// Splits what a server sent into `(code, body)` messages.
pub fn server_messages(mut bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    while let Some((&[code, a, b, c, d], rest)) = bytes.split_first_chunk::<5>() {
        let (body, rest) = rest.split_at(u32::from_be_bytes([a, b, c, d]) as usize - 4);
        messages.push((code, body.to_vec()));
        bytes = rest;
    }
    messages
}

/// Polls a future once.
pub fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Runs a future that is expected to finish without waiting.
pub fn now<F: Future>(future: F) -> F::Output {
    match poll(std::pin::pin!(future)) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the future is waiting on the server"),
    }
}

/// A backend message.
pub fn message(code: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![code];
    message.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(body);
    message
}

pub fn ready(status: u8) -> Vec<u8> {
    message(b'Z', &[status])
}

pub fn command_complete(tag: &str) -> Vec<u8> {
    message(b'C', format!("{tag}\0").as_bytes())
}

pub fn data_row(values: &[Option<&str>]) -> Vec<u8> {
    let mut body = (values.len() as u16).to_be_bytes().to_vec();
    for value in values {
        match value {
            Some(value) => {
                body.extend_from_slice(&(value.len() as i32).to_be_bytes());
                body.extend_from_slice(value.as_bytes());
            }
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    message(b'D', &body)
}

pub fn row_description(names: &[&str]) -> Vec<u8> {
    let mut body = (names.len() as u16).to_be_bytes().to_vec();
    for name in names {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0,
        ]);
    }
    message(b'T', &body)
}

/// One simple-query result set with a single row.
pub fn select(value: &str, status: u8) -> Vec<u8> {
    let parts = [
        row_description(&["value"]),
        data_row(&[Some(value)]),
        command_complete("SELECT 1"),
        ready(status),
    ];
    parts.concat()
}

fn fields(code: u8, severity: &str, sqlstate: &str, text: &str) -> Vec<u8> {
    message(
        code,
        format!("S{severity}\0V{severity}\0C{sqlstate}\0M{text}\0\0").as_bytes(),
    )
}

pub fn error(sqlstate: &str, text: &str) -> Vec<u8> {
    fields(b'E', "ERROR", sqlstate, text)
}

pub fn fatal(sqlstate: &str, text: &str) -> Vec<u8> {
    fields(b'E', "FATAL", sqlstate, text)
}

pub fn notice(text: &str) -> Vec<u8> {
    fields(b'N', "NOTICE", "00000", text)
}

pub fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    message(b'S', format!("{name}\0{value}\0").as_bytes())
}

pub fn notification(process_id: u32, channel: &str, payload: &str) -> Vec<u8> {
    let mut body = process_id.to_be_bytes().to_vec();
    body.extend_from_slice(format!("{channel}\0{payload}\0").as_bytes());
    message(b'A', &body)
}

pub fn authentication(code: u32, body: &[u8]) -> Vec<u8> {
    message(b'R', &[&code.to_be_bytes()[..], body].concat())
}

/// Decodes the single certificate in a PEM file to DER.
pub fn pem_to_der(pem: &str) -> Vec<u8> {
    let value = |byte: u8| match byte {
        b'A'..=b'Z' => byte - b'A',
        b'a'..=b'z' => byte - b'a' + 26,
        b'0'..=b'9' => byte - b'0' + 52,
        b'+' => 62,
        _ => 63,
    };
    let base64 = pem.lines().filter(|line| !line.starts_with("-----"));
    let symbols = base64.flat_map(str::bytes).filter(|&byte| byte != b'=');
    let symbols: Vec<u8> = symbols.map(value).collect();

    let mut der = Vec::new();
    for chunk in symbols.chunks(4) {
        let symbols = chunk.iter();
        let bits = symbols.fold(0u32, |bits, &symbol| (bits << 6) | symbol as u32);
        let bits = bits << (6 * (4 - chunk.len()));
        der.extend_from_slice(&bits.to_be_bytes()[1..chunk.len()]);
    }
    der
}

pub fn column_names(columns: RowDescription<'_>) -> Vec<String> {
    let names = columns.iter().map(|column| column.name.to_owned());
    names.collect()
}

/// A one-line description of an asynchronous message.
pub fn describe(message: AsyncMessage<'_>) -> String {
    match message {
        AsyncMessage::Notice(notice) => format!("notice {}", notice.message()),
        AsyncMessage::Notification(n) => format!("notify {} {}", n.channel, n.payload),
        AsyncMessage::ParameterStatus(p) => format!("parameter {}={}", p.name, p.value),
    }
}
