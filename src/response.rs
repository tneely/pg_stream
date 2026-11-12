//! Reading the replies to one cycle.

use crate::{
    Connection, Error, Frontend, ProtocolError, Result, Transport,
    backend::{
        CommandComplete, CopyResponse, DataRow, Message, ParameterDescription, RowDescription,
        TransactionStatus, code, function_result,
    },
};

/// The replies to one cycle: everything the server sends up to and including
/// the next ReadyForQuery. A cycle is whatever was sent up to a Sync, or one
/// simple Query, or one FunctionCall.
///
/// Each method reads the reply to one message you sent, in the order you sent
/// them, and applies the protocol's rules so you don't have to:
///
/// - An ErrorResponse becomes [`Error::Server`]. The server then skips every
///   message up to the Sync, so the replies you would have read next never
///   arrive; asking for them returns [`Error::Aborted`] without reading.
/// - Notices, notifications and parameter changes can arrive between any two
///   replies. They are set aside for [`Connection::async_messages`].
/// - A reply that does not match is reported as
///   [`ProtocolError::Unexpected`] and left unread, so a miscounted pipeline
///   cannot swallow another request's reply.
/// - Dropping the response early is safe: the connection discards the rest of
///   the cycle before it reads anything else. That covers early returns,
///   panics and cancelled futures alike.
///
/// ```no_run
/// # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
/// use pg_stream::{Format, Frontend, params};
///
/// conn.parse("", "SELECT name FROM users WHERE id = $1", &[])
///     .bind("", "", params![7i32], Format::Text)
///     .execute("", 0)
///     .sync();
///
/// let mut response = conn.response();
/// response.parse_complete().await?;
/// response.bind_complete().await?;
/// while let Some(row) = response.row().await? {
///     println!("{:?}", row.get(0));
/// }
/// response.complete().await?;
/// response.finish().await?;
/// # Ok(())
/// # }
/// ```
///
/// When the shape of the response is not known in advance, as with a simple
/// Query holding several statements, iterate it with [`next`](Self::next).
///
/// A response claims exactly one ReadyForQuery. If you drop one before the
/// Sync that ends its cycle has been queued, queue that Sync: the server
/// answers nothing else once a cycle has failed.
#[derive(Debug)]
#[must_use = "replies are only read by calling methods; `finish` reports errors not yet seen"]
pub struct Response<'c, S> {
    conn: &'c mut Connection<S>,
    phase: Phase,
    /// An error [`finish`](Self::finish) has read and not yet returned. It is
    /// kept here so that it survives a `finish` that is dropped midway.
    unreported: Option<Error>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Replies are still expected.
    Open,
    /// An ErrorResponse has been reported; only ReadyForQuery is still to come.
    Failed,
    /// ReadyForQuery has been read.
    Done,
}

/// How an Execute ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion<'a> {
    /// The command ran to completion.
    Command(CommandComplete<'a>),
    /// The query string was empty.
    EmptyQuery,
    /// The Execute's row limit was reached; Execute the portal again for more.
    Suspended,
}

impl Completion<'_> {
    /// The command's row count, if it finished and has one.
    pub fn rows(&self) -> Option<u64> {
        match self {
            Completion::Command(command) => command.rows(),
            Completion::EmptyQuery | Completion::Suspended => None,
        }
    }
}

/// The code and body length of the next reply to `$response`, which is not
/// consumed. Fails as [`Response::wait`] does.
///
/// A macro, so that the check for a reply that has already arrived lands in
/// the caller's own body. Rows usually arrive many to a read, and a future
/// awaited there is not reliably inlined.
macro_rules! next_reply {
    ($response:ident, $expected:expr) => {
        match $response.buffered() {
            Some(reply) => reply,
            None => $response.wait($expected).await?,
        }
    };
}

impl<'c, S> Response<'c, S> {
    pub(crate) fn new(conn: &'c mut Connection<S>) -> Self {
        Self {
            conn,
            phase: Phase::Open,
            unreported: None,
        }
    }

    /// The connection's queue of messages not yet sent. See
    /// [`Connection::buf`].
    pub fn buf(&mut self) -> &mut Vec<u8> {
        self.conn.buf()
    }
}

impl<S: Transport> Response<'_, S> {
    /// The reply to Parse.
    pub async fn parse_complete(&mut self) -> Result<()> {
        self.expect(code::PARSE_COMPLETE, empty).await
    }

    /// The reply to Bind.
    pub async fn bind_complete(&mut self) -> Result<()> {
        self.expect(code::BIND_COMPLETE, empty).await
    }

    /// The reply to Close.
    pub async fn close_complete(&mut self) -> Result<()> {
        self.expect(code::CLOSE_COMPLETE, empty).await
    }

    /// The first reply to Describe of a statement. The second is
    /// [`row_description`](Self::row_description).
    pub async fn parameter_description(&mut self) -> Result<ParameterDescription<'_>> {
        let parse = ParameterDescription::try_parse;
        self.expect(code::PARAMETER_DESCRIPTION, parse).await
    }

    /// The reply to Describe of a portal, the second reply to Describe of a
    /// statement, or the start of a result set in a simple Query. `None` means
    /// the statement returns no rows.
    pub async fn row_description(&mut self) -> Result<Option<RowDescription<'_>>> {
        const EXPECTED: &str = "RowDescription or NoData";
        let (code, body_len) = next_reply!(self, EXPECTED);
        match code {
            code::ROW_DESCRIPTION => {
                let body = self.conn.take(code, body_len);
                parsed(code, RowDescription::try_parse(body)).map(Some)
            }
            code::NO_DATA => {
                self.conn.take(code, body_len);
                Ok(None)
            }
            _ => Err(self.reject(code, body_len, EXPECTED)),
        }
    }

    /// The next row of an Execute or simple Query. `None` means the rows have
    /// ended and [`complete`](Self::complete) is next.
    ///
    /// A reply that comes before the rows, such as the RowDescription of a
    /// simple Query, has to be read first.
    pub async fn row(&mut self) -> Result<Option<DataRow<'_>>> {
        let (code, body_len) = next_reply!(self, "DataRow");
        match code {
            code::DATA_ROW => Ok(Some(DataRow::parse(self.conn.take(code, body_len))?)),
            code::COMMAND_COMPLETE | code::EMPTY_QUERY_RESPONSE | code::PORTAL_SUSPENDED => {
                Ok(None)
            }
            _ => Err(self.reject(code, body_len, "DataRow")),
        }
    }

    /// The end of an Execute or of one statement of a simple Query. Rows and
    /// COPY data not yet read are skipped.
    pub async fn complete(&mut self) -> Result<Completion<'_>> {
        const EXPECTED: &str = "CommandComplete";
        let (code, body_len) = loop {
            let (code, body_len) = next_reply!(self, EXPECTED);
            match code {
                code::DATA_ROW | code::COPY_DATA | code::COPY_DONE => {
                    self.conn.take(code, body_len);
                }
                _ => break (code, body_len),
            }
        };
        match code {
            code::COMMAND_COMPLETE => {
                let body = self.conn.take(code, body_len);
                parsed(code, CommandComplete::try_parse(body)).map(Completion::Command)
            }
            code::EMPTY_QUERY_RESPONSE => {
                self.conn.take(code, body_len);
                Ok(Completion::EmptyQuery)
            }
            code::PORTAL_SUSPENDED => {
                self.conn.take(code, body_len);
                Ok(Completion::Suspended)
            }
            _ => Err(self.reject(code, body_len, EXPECTED)),
        }
    }

    /// The reply to a FunctionCall; `None` is SQL NULL.
    pub async fn function_result(&mut self) -> Result<Option<&[u8]>> {
        const CODE: u8 = code::FUNCTION_CALL_RESPONSE;
        self.expect(CODE, function_result).await
    }

    /// The start of a COPY FROM STDIN. The server now waits for
    /// [`copy_data`](Frontend::copy_data), ended by
    /// [`copy_done`](Frontend::copy_done) or
    /// [`copy_fail`](Frontend::copy_fail), and then replies with
    /// [`complete`](Self::complete).
    ///
    /// A Sync sent before the COPY ends is ignored by the server. If the COPY
    /// was started by an Execute, follow `copy_done` or `copy_fail` with
    /// another.
    pub async fn copy_in_response(&mut self) -> Result<CopyResponse<'_>> {
        let parse = CopyResponse::try_parse;
        self.expect(code::COPY_IN_RESPONSE, parse).await
    }

    /// The start of a COPY TO STDOUT. Read the data with
    /// [`copy_out_data`](Self::copy_out_data).
    pub async fn copy_out_response(&mut self) -> Result<CopyResponse<'_>> {
        let parse = CopyResponse::try_parse;
        self.expect(code::COPY_OUT_RESPONSE, parse).await
    }

    /// The start of a COPY in both directions, which is how a server streams
    /// replication. Read its data with [`copy_out_data`](Self::copy_out_data)
    /// and send your own with [`copy_data`](Frontend::copy_data), ended by
    /// [`copy_done`](Frontend::copy_done).
    pub async fn copy_both_response(&mut self) -> Result<CopyResponse<'_>> {
        let parse = CopyResponse::try_parse;
        self.expect(code::COPY_BOTH_RESPONSE, parse).await
    }

    /// The next chunk of a COPY TO STDOUT, or of the server's side of a COPY
    /// in both directions. `None` means the data has ended and
    /// [`complete`](Self::complete) is next.
    pub async fn copy_out_data(&mut self) -> Result<Option<&[u8]>> {
        const EXPECTED: &str = "CopyData or CopyDone";
        let (code, body_len) = next_reply!(self, EXPECTED);
        match code {
            code::COPY_DATA => Ok(Some(self.conn.take(code, body_len))),
            code::COPY_DONE => {
                self.conn.take(code, body_len);
                Ok(None)
            }
            _ => Err(self.reject(code, body_len, EXPECTED)),
        }
    }

    /// The next reply, whatever it is, ending with ReadyForQuery and then
    /// `None`.
    ///
    /// Unlike the other methods this does not interpret the reply: an
    /// ErrorResponse is returned as a message, not as an error.
    pub async fn next(&mut self) -> Result<Option<Message<'_>>> {
        if self.phase == Phase::Done {
            return Ok(None);
        }
        let (code, body_len) = match self.conn.buffered_reply() {
            Some(reply) => reply,
            None => self.conn.wait_for_reply().await?,
        };
        match code {
            code::READY_FOR_QUERY => self.phase = Phase::Done,
            code::ERROR_RESPONSE => self.phase = Phase::Failed,
            _ => {}
        }
        let message = Message::parse(code, self.conn.take(code, body_len))?;
        Ok(Some(message))
    }

    /// Reads through ReadyForQuery, skipping any replies not yet read.
    ///
    /// If one of the skipped replies is an ErrorResponse, it is returned once
    /// the cycle has ended, so a failure cannot go unnoticed. That includes
    /// errors raised by the Sync itself, such as a deferred constraint failing
    /// at commit.
    ///
    /// A COPY FROM STDIN that has not been ended cannot be skipped, since the
    /// server is waiting for the client. That is reported as an unexpected
    /// CopyInResponse rather than waited on.
    pub async fn finish(&mut self) -> Result<TransactionStatus> {
        while self.phase != Phase::Done {
            let (code, body_len) = match self.conn.buffered_reply() {
                Some(reply) => reply,
                None => self.wait_to_skip().await?,
            };
            if code == code::ERROR_RESPONSE && self.phase == Phase::Open {
                self.unreported = Some(self.reject(code, body_len, "ReadyForQuery"));
                continue;
            }
            if self.conn.starts_open_copy(code) {
                return Err(Error::unexpected(code, "ReadyForQuery"));
            }
            if code == code::READY_FOR_QUERY {
                self.phase = Phase::Done;
            }
            self.conn.take(code, body_len);
        }
        match self.unreported.take() {
            Some(error) => Err(error),
            None => Ok(self.conn.transaction_status()),
        }
    }

    /// Sends everything queued. See [`Connection::flush`].
    pub async fn flush(&mut self) -> Result<()> {
        self.conn.flush().await
    }

    /// The next reply, if it has already arrived: its code and body length.
    /// It is not consumed.
    #[inline]
    fn buffered(&self) -> Option<(u8, usize)> {
        match self.phase {
            Phase::Open => self.conn.buffered_reply(),
            Phase::Failed | Phase::Done => None,
        }
    }

    /// Waits for the next reply and reports its code and body length, without
    /// consuming it.
    async fn wait(&mut self, expected: &'static str) -> Result<(u8, usize)> {
        match self.phase {
            Phase::Open => self.conn.wait_for_reply().await,
            Phase::Failed => Err(Error::Aborted),
            Phase::Done => Err(Error::unexpected(code::READY_FOR_QUERY, expected)),
        }
    }

    /// Waits for the next reply on behalf of [`finish`](Self::finish), unless
    /// the server is not going to send one.
    async fn wait_to_skip(&mut self) -> Result<(u8, usize)> {
        self.conn.check_skippable()?;
        let reply = self.conn.wait_for_reply().await;
        // A server that hangs up has usually said why first, and that is the
        // error worth reporting.
        reply.map_err(|e| self.unreported.take().unwrap_or(e))
    }

    /// Consumes the next reply, which must have the code `want`, and parses
    /// its body.
    async fn expect<'s, T>(
        &'s mut self,
        want: u8,
        parse: impl FnOnce(&'s [u8]) -> Option<T>,
    ) -> Result<T> {
        let (code, body_len) = next_reply!(self, code::name(want));
        match code == want {
            true => parsed(code, parse(self.conn.take(code, body_len))),
            false => Err(self.reject(code, body_len, code::name(want))),
        }
    }

    /// Turns a reply that is not the one asked for into an error. An
    /// ErrorResponse is consumed; anything else is left for the caller.
    fn reject(&mut self, code: u8, body_len: usize, expected: &'static str) -> Error {
        if code != code::ERROR_RESPONSE {
            return Error::unexpected(code, expected);
        }
        self.phase = Phase::Failed;
        self.conn.take_error(body_len)
    }
}

fn parsed<T>(code: u8, value: Option<T>) -> Result<T> {
    value.ok_or(ProtocolError::Malformed { code }.into())
}

/// Parses the body of a reply that has none.
fn empty(body: &[u8]) -> Option<()> {
    body.is_empty().then_some(())
}

/// Messages can be queued while a response is being read, which is how COPY
/// data is sent and how a pipeline is extended as its replies come in.
impl<S> Frontend for Response<'_, S> {
    type Buf = Vec<u8>;

    fn with_buf<R>(&mut self, write: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        write(self.buf())
    }
}
