//! A small asynchronous client built on pg_stream.
//!
//! One task owns the socket, and everything else talks to it through handles:
//!
//! - A [`Connection`] owns the socket and runs as a task. It writes the
//!   requests it is sent and hands each reply to whoever is waiting for it.
//! - A [`Client`] is a handle to that task. It encodes requests itself and
//!   sends the bytes over, so any number of tasks can share one connection and
//!   whatever they ask for at the same time is pipelined.
//! - [`Statement`], [`Row`], [`ToSql`] and [`FromSql`] are what a client adds
//!   on top of the protocol: names for prepared statements, and a mapping
//!   between SQL values and Rust types.
//!
//! Run it against a server with the usual libpq variables:
//!
//! ```sh
//! PGHOST=localhost PGPORT=5432 PGUSER=postgres cargo run --example client
//! ```
//!
//! To stay short it leaves out TLS, transactions, COPY, cancellation,
//! backpressure and most types.

use std::{
    any::type_name,
    collections::VecDeque,
    env, fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use pg_stream::{
    Format, Frontend, Message, Oid, ProtocolError, ServerError,
    backend::{self, DataRow, Field, Frame, code},
    frontend::{IsNull, try_params_with},
    oid,
    startup::{Session, Startup},
};
use tokio::{net::TcpStream, sync::mpsc};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let var = |name: &str, default: &str| env::var(name).unwrap_or(default.into());
    let addr = format!("{}:{}", var("PGHOST", "localhost"), var("PGPORT", "5432"));
    let user = var("PGUSER", "postgres");
    let (database, password) = (env::var("PGDATABASE").ok(), env::var("PGPASSWORD").ok());

    let mut startup = Startup::new(&user);
    if let Some(database) = &database {
        startup.database(database);
    }
    if let Some(password) = &password {
        startup.password(password);
    }
    let (client, mut connection) = connect(&addr, &mut startup).await?;
    let version = connection.parameter("server_version").unwrap_or("?");
    println!("connected to PostgreSQL {version}");

    // Nothing happens until the connection is running somewhere.
    let mut notifications = connection.notifications();
    let connection = tokio::spawn(connection.run());

    let create = "CREATE TEMP TABLE planets (id int4 PRIMARY KEY, name text NOT NULL, moons int8)";
    client.batch_execute(create).await?;

    // A request is sent when it is made, not when it is awaited. Making five
    // and then awaiting five is a pipeline: nothing waits on a round trip.
    let insert = "INSERT INTO planets VALUES ($1, $2, $3)";
    let insert = client.prepare(insert).await?;
    let planets = [
        (1, "Mercury", Some(0i64)),
        (2, "Venus", Some(0)),
        (3, "Earth", Some(1)),
        (4, "Mars", Some(2)),
        (5, "Planet Nine", None),
    ];
    let mut inserts = Vec::new();
    for (id, name, moons) in &planets {
        inserts.push(client.execute(&insert, &[id, name, moons]));
    }
    let mut inserted = 0;
    for insert in inserts {
        inserted += insert.await?;
    }
    println!("inserted {inserted} planets");

    let select = "SELECT name, moons FROM planets WHERE moons IS NOT DISTINCT FROM $1 ORDER BY id";
    let by_moons = client.prepare(select).await?;
    let columns = by_moons.columns().iter().map(|column| column.name.as_str());
    println!("columns: {}", columns.collect::<Vec<_>>().join(", "));
    for moons in [Some(0i64), None] {
        for row in client.query(&by_moons, &[&moons]).await? {
            let (name, moons): (String, Option<i64>) = (row.get(0)?, row.get(1)?);
            println!("{name}: {moons:?}");
        }
    }

    // Handles are cheap to clone and work from any task. All of them share
    // the one connection.
    let task = tokio::spawn({
        let (client, by_moons) = (client.clone(), by_moons.clone());
        async move { client.query(&by_moons, &[&Some(2i64)]).await }
    });
    let rows = task.await.expect("the task panicked")?;
    let name: String = rows[0].get(0)?;
    println!("from another task: {name} has two moons");

    // A request the server refuses fails alone. Each one is a cycle of its
    // own, so the requests pipelined around it are answered as usual.
    let count = client.prepare("SELECT count(*) FROM planets").await?;
    let duplicate = client.execute(&insert, &[&3i32, &"Earth again", &None::<i64>]);
    let total = client.query(&count, &[]);
    match duplicate.await {
        Err(Error::Db(refusal)) => println!("refused: {refusal}"),
        other => println!("expected the server to refuse, got {other:?}"),
    }
    let total: i64 = total.await?[0].get(0)?;
    println!("still {total} planets");

    // A value that does not fit its parameter never reaches the server.
    match client.execute(&insert, &[&"six", &"Pluto", &5i64]).await {
        Err(Error::Type(mismatch)) => println!("not sent: {mismatch}"),
        other => println!("expected a type error, got {other:?}"),
    }

    let notify = "LISTEN news; NOTIFY news, 'hello from pg_stream'";
    client.batch_execute(notify).await?;
    if let Some(Notification { channel, payload }) = notifications.recv().await {
        println!("notified on {channel}: {payload}");
    }

    // The connection runs until the last handle to it is gone.
    drop((client, insert, by_moons, count));
    connection.await.expect("the connection task panicked")
}

/// Opens a session and returns a handle to it, along with the [`Connection`]
/// that has to be [run](Connection::run) for the handle to do anything.
pub async fn connect(addr: &str, startup: &mut Startup<'_>) -> Result<(Client, Connection), Error> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (conn, session) = startup.connect(stream).await?;

    let (requests, receiver) = mpsc::unbounded_channel();
    let client = Client {
        requests,
        statements: Arc::default(),
    };
    let connection = Connection {
        conn,
        session,
        requests: receiver,
        waiting: VecDeque::new(),
        notifications: None,
    };
    Ok((client, connection))
}

/// One cycle of messages on its way to the server, and where its replies go.
struct Request {
    /// Whole messages that end with a Sync, or are a single simple Query, so
    /// that the server answers them with exactly one ReadyForQuery.
    messages: Vec<u8>,
    replies: mpsc::UnboundedSender<Frame>,
}

/// The task that owns the socket.
pub struct Connection {
    conn: pg_stream::Connection<TcpStream>,
    session: Session,
    requests: mpsc::UnboundedReceiver<Request>,
    /// Where the replies to each request sent and not yet answered go, oldest
    /// first. The server answers requests in the order it was sent them, so
    /// this is all the bookkeeping there is.
    waiting: VecDeque<mpsc::UnboundedSender<Frame>>,
    notifications: Option<mpsc::UnboundedSender<Notification>>,
}

impl Connection {
    /// A parameter the server reported at startup, such as `server_version`.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.session.parameter(name)
    }

    /// Delivers what the session is notified of to the receiver returned.
    pub fn notifications(&mut self) -> mpsc::UnboundedReceiver<Notification> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.notifications = Some(sender);
        receiver
    }

    /// Runs the connection until every [`Client`] and [`Statement`] is gone
    /// and everything they asked for has been answered.
    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                request = self.requests.recv() => {
                    let Some(request) = request else { break };
                    self.queue(request);
                    // Whatever else has been asked for goes in the same write.
                    while let Ok(request) = self.requests.try_recv() {
                        self.queue(request);
                    }
                }
                // Waiting for a frame is also what sends the requests queued
                // above. When the other branch wins this future is dropped
                // mid-read, which loses nothing.
                frame = self.conn.recv_frame() => self.route(frame?)?,
            }
        }
        // The handles are gone, but the futures they returned may still be
        // waiting, so every request that was made is answered before leaving.
        while !self.waiting.is_empty() {
            let frame = self.conn.recv_frame().await?;
            self.route(frame)?;
        }
        self.conn.terminate();
        Ok(self.conn.flush().await?)
    }

    fn queue(&mut self, request: Request) {
        self.conn.buf().extend_from_slice(&request.messages);
        self.waiting.push_back(request.replies);
    }

    /// Hands a frame from the server to whoever it is for.
    fn route(&mut self, frame: Frame) -> Result<(), Error> {
        if code::is_async(frame.code) {
            return self.unprompted(&frame);
        }
        let Some(replies) = self.waiting.front() else {
            // Nobody asked for anything: the server is ending the session.
            return Err(server_error(&frame));
        };
        let last = frame.code == code::READY_FOR_QUERY;
        // A receiver that is gone means the request was abandoned. Its replies
        // still had to be taken off the wire, and now they have been.
        let _ = replies.send(frame);
        if last {
            self.waiting.pop_front();
        }
        Ok(())
    }

    /// Deals with a message the server sent of its own accord.
    fn unprompted(&mut self, frame: &Frame) -> Result<(), Error> {
        match (frame.parse()?, &self.notifications) {
            (Message::NotificationResponse(notification), Some(listener)) => {
                let _ = listener.send(notification.into());
            }
            (Message::NoticeResponse(notice), _) => eprintln!("{notice}"),
            // A notification nobody listens for, or a setting that changed.
            _ => {}
        }
        Ok(())
    }
}

/// A NOTIFY delivered to the session.
#[derive(Debug)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
}

impl From<backend::Notification<'_>> for Notification {
    fn from(notification: backend::Notification<'_>) -> Self {
        let channel = notification.channel.into();
        let payload = notification.payload.into();
        Self { channel, payload }
    }
}

/// A handle to a [`Connection`].
///
/// Requests are sent when its methods are called, not when the futures they
/// return are first polled. Calling several before awaiting any pipelines
/// them, and dropping a future abandons the reply without disturbing the
/// requests around it.
#[derive(Clone)]
pub struct Client {
    requests: mpsc::UnboundedSender<Request>,
    /// How many statements have been prepared, to give each a name.
    statements: Arc<AtomicUsize>,
}

impl Client {
    /// Runs SQL that takes no parameters and whose rows are of no interest.
    /// It may hold several statements.
    pub fn batch_execute(&self, sql: &str) -> impl Future<Output = Result<(), Error>> + use<> {
        let mut messages = Vec::new();
        messages.query(sql);
        let replies = self.send(messages);
        async move {
            let mut replies = replies?;
            while replies.next().await?.is_some() {}
            Ok(())
        }
    }

    /// Prepares a statement, and learns its parameter and column types.
    pub fn prepare(&self, sql: &str) -> impl Future<Output = Result<Statement, Error>> + use<> {
        let name = format!("s{}", self.statements.fetch_add(1, Ordering::Relaxed));
        let mut messages = Vec::new();
        let parsed = messages.parse(&name, sql, &[]);
        parsed.describe_statement(&name).sync();
        let replies = self.send(messages);
        // Made before the server has answered, so that dropping the future at
        // any point still closes whatever the server may have prepared.
        let mut statement = StatementInner::new(name, self.requests.clone());
        async move {
            let mut replies = replies?;
            while let Some(frame) = replies.next().await? {
                statement.describe(frame.parse()?);
            }
            Ok(Statement(Arc::new(statement)))
        }
    }

    /// Runs a prepared statement and returns its rows.
    pub fn query(
        &self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Vec<Row>, Error>> + use<> {
        let replies = self.bind_and_execute(statement, params);
        let columns = statement.0.columns.clone();
        async move {
            let mut replies = replies?;
            let mut rows = Vec::new();
            while let Some(frame) = replies.next().await? {
                if frame.code == code::DATA_ROW {
                    rows.push(Row::new(&frame.body, columns.clone())?);
                }
            }
            Ok(rows)
        }
    }

    /// Runs a prepared statement and returns how many rows it affected.
    pub fn execute(
        &self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<u64, Error>> + use<> {
        let replies = self.bind_and_execute(statement, params);
        async move {
            let mut replies = replies?;
            let mut affected = 0;
            while let Some(frame) = replies.next().await? {
                if let Message::CommandComplete(done) = frame.parse()? {
                    affected = done.rows().unwrap_or(0);
                }
            }
            Ok(affected)
        }
    }

    fn bind_and_execute(
        &self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> Result<Replies, Error> {
        let statement = &statement.0;
        let (expected, given) = (statement.params.len(), params.len());
        if expected != given {
            let mismatch = format!("{expected} parameters expected, {given} given");
            return Err(Error::Type(mismatch));
        }

        // Each value is written straight into the Bind message, in binary,
        // according to the type the server gave its parameter. If one does
        // not fit, `try_bind` leaves the buffer as it was.
        let values = try_params_with(Format::Binary, |values| {
            for (param, &ty) in params.iter().zip(&statement.params) {
                values.try_write(|buf| param.to_sql(ty, buf))?;
            }
            Ok::<_, Error>(())
        });
        let mut messages = Vec::new();
        let bound = messages.try_bind("", &statement.name, values, Format::Binary)?;
        bound.execute("", 0).sync();
        self.send(messages)
    }

    /// Sends one cycle of messages and returns the replies to come.
    fn send(&self, messages: Vec<u8>) -> Result<Replies, Error> {
        let (replies, receiver) = mpsc::unbounded_channel();
        let request = Request { messages, replies };
        self.requests.send(request).or(Err(Error::Closed))?;
        Ok(Replies(receiver))
    }
}

/// The replies to one request, as the connection hands them over.
struct Replies(mpsc::UnboundedReceiver<Frame>);

impl Replies {
    /// The next reply, or `None` once the cycle has ended. An error from the
    /// server ends it at once: the replies that are left are the connection's
    /// to deal with.
    async fn next(&mut self) -> Result<Option<Frame>, Error> {
        let frame = self.0.recv().await.ok_or(Error::Closed)?;
        match frame.code {
            code::READY_FOR_QUERY => Ok(None),
            code::ERROR_RESPONSE => Err(server_error(&frame)),
            _ => Ok(Some(frame)),
        }
    }
}

/// The error a frame carries if it is an ErrorResponse, as it should be.
fn server_error(frame: &Frame) -> Error {
    let (code, expected) = (frame.code, "ErrorResponse");
    let unexpected = ProtocolError::Unexpected { code, expected };
    match frame.parse() {
        Ok(Message::ErrorResponse(error)) => Error::Db(error.to_error()),
        Ok(_) => unexpected.into(),
        Err(malformed) => malformed.into(),
    }
}

/// A prepared statement. The server keeps it until its last handle is dropped.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

struct StatementInner {
    name: String,
    params: Vec<Oid>,
    columns: Arc<[Column]>,
    requests: mpsc::UnboundedSender<Request>,
}

impl Statement {
    /// The columns of the rows the statement returns.
    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

impl StatementInner {
    fn new(name: String, requests: mpsc::UnboundedSender<Request>) -> Self {
        Self {
            name,
            params: Vec::new(),
            columns: Arc::default(),
            requests,
        }
    }

    /// Takes in what a reply to Parse or Describe says about the statement.
    fn describe(&mut self, reply: Message<'_>) {
        match reply {
            Message::ParameterDescription(types) => self.params = types.iter().collect(),
            Message::RowDescription(fields) => {
                self.columns = fields.iter().map(Column::from).collect()
            }
            // ParseComplete, or NoData from a statement without rows.
            _ => {}
        }
    }
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        let mut messages = Vec::new();
        messages.close_statement(&self.name).sync();
        // Nobody waits for the reply. The connection reads it all the same.
        let (replies, _) = mpsc::unbounded_channel();
        let _ = self.requests.send(Request { messages, replies });
    }
}

/// A column of a result set.
pub struct Column {
    pub name: String,
    pub type_oid: Oid,
}

impl From<Field<'_>> for Column {
    fn from(field: Field<'_>) -> Self {
        let (name, type_oid) = (field.name.into(), field.type_oid);
        Self { name, type_oid }
    }
}

/// A row of a result set.
pub struct Row {
    columns: Arc<[Column]>,
    values: Vec<Option<Bytes>>,
}

impl Row {
    fn new(body: &Bytes, columns: Arc<[Column]>) -> Result<Self, Error> {
        let row = DataRow::parse(body)?;
        // Each value is a view into the frame, which is a view into the read
        // buffer of the connection. No data is copied to make a row.
        let view = |value: &[u8]| body.slice_ref(value);
        let values = row.iter().map(|value| value.map(view)).collect();
        Ok(Self { columns, values })
    }

    /// The value of a column, as any type its SQL type converts to.
    pub fn get<T: FromSql>(&self, index: usize) -> Result<T, Error> {
        let missing = || Error::Type(format!("there is no column {index}"));
        let column = self.columns.get(index).ok_or_else(missing)?;
        let value = self.values.get(index).ok_or_else(missing)?;
        T::from_sql(column.type_oid, value.as_deref())
    }
}

/// A Rust value that can be sent as a parameter, in binary.
///
/// `Sync`, so that a list of parameters can be held across an `.await` in a
/// task that moves between threads.
pub trait ToSql: Sync {
    /// Appends the value as the SQL type `ty` represents it.
    fn to_sql(&self, ty: Oid, buf: &mut Vec<u8>) -> Result<IsNull, Error>;
}

/// A Rust value that can be read from a column, in binary.
pub trait FromSql: Sized {
    /// Reads a value of the SQL type `ty`. `None` is NULL.
    fn from_sql(ty: Oid, raw: Option<&[u8]>) -> Result<Self, Error>;
}

/// Fails unless `ty` is one of the SQL types that `T` converts to and from.
fn check<T: ?Sized>(ty: Oid, accepted: &[Oid]) -> Result<(), Error> {
    if accepted.contains(&ty) {
        return Ok(());
    }
    let rust = type_name::<T>();
    Err(Error::Type(format!(
        "{rust} does not convert to the type with OID {ty}"
    )))
}

fn not_null<T>(raw: Option<&[u8]>) -> Result<&[u8], Error> {
    raw.ok_or_else(|| Error::Type(format!("NULL does not convert to {}", type_name::<T>())))
}

macro_rules! number {
    ($($rust:ty => $sql:expr),*) => {$(
        impl ToSql for $rust {
            fn to_sql(&self, ty: Oid, buf: &mut Vec<u8>) -> Result<IsNull, Error> {
                check::<Self>(ty, &[$sql])?;
                buf.extend_from_slice(&self.to_be_bytes());
                Ok(IsNull::No)
            }
        }

        impl FromSql for $rust {
            fn from_sql(ty: Oid, raw: Option<&[u8]>) -> Result<Self, Error> {
                check::<Self>(ty, &[$sql])?;
                let bytes = not_null::<Self>(raw)?.try_into();
                let wrong_size = || Error::Type(format!("not a {}", type_name::<Self>()));
                Ok(Self::from_be_bytes(bytes.map_err(|_| wrong_size())?))
            }
        }
    )*};
}

number!(i32 => oid::INT4, i64 => oid::INT8, f64 => oid::FLOAT8);

/// The types whose binary form is their text.
const TEXT: &[Oid] = &[oid::TEXT, oid::VARCHAR, oid::BPCHAR, oid::NAME];

impl ToSql for str {
    fn to_sql(&self, ty: Oid, buf: &mut Vec<u8>) -> Result<IsNull, Error> {
        check::<Self>(ty, TEXT)?;
        buf.extend_from_slice(self.as_bytes());
        Ok(IsNull::No)
    }
}

impl FromSql for String {
    fn from_sql(ty: Oid, raw: Option<&[u8]>) -> Result<Self, Error> {
        check::<Self>(ty, TEXT)?;
        let text = String::from_utf8(not_null::<Self>(raw)?.to_vec());
        text.map_err(|e| Error::Type(e.to_string()))
    }
}

impl<T: ToSql + ?Sized> ToSql for &T {
    fn to_sql(&self, ty: Oid, buf: &mut Vec<u8>) -> Result<IsNull, Error> {
        (**self).to_sql(ty, buf)
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self, ty: Oid, buf: &mut Vec<u8>) -> Result<IsNull, Error> {
        match self {
            Some(value) => value.to_sql(ty, buf),
            None => Ok(IsNull::Yes),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(ty: Oid, raw: Option<&[u8]>) -> Result<Self, Error> {
        raw.map(|raw| T::from_sql(ty, Some(raw))).transpose()
    }
}

#[derive(Debug)]
pub enum Error {
    /// The connection failed, or could not be opened.
    Connection(pg_stream::Error),
    /// The server refused a request. The connection is as good as it was.
    Db(ServerError),
    /// The connection is no longer running.
    Closed,
    /// A Rust value and a SQL type that do not go together.
    Type(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(e) => write!(f, "{e}"),
            Error::Db(e) => write!(f, "{e}"),
            Error::Closed => f.write_str("the connection is closed"),
            Error::Type(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for Error {}

impl From<pg_stream::Error> for Error {
    fn from(e: pg_stream::Error) -> Self {
        match e {
            pg_stream::Error::Server(e) => Error::Db(e),
            e => Error::Connection(e),
        }
    }
}

impl From<ProtocolError> for Error {
    fn from(e: ProtocolError) -> Self {
        Error::Connection(e.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Connection(e.into())
    }
}
