use std::{borrow::Cow, ops::Range};

use bytes::Bytes;

use crate::messages::backend::{self, PgFrame};

/// A zero-copy representation of a Postgres ErrorResponse or NoticeResponse
pub struct PgErrorResponse {
    body: Bytes,
    local_severity: Option<Range<usize>>,    // S
    severity: Option<Range<usize>>,          // V
    code: Option<Range<usize>>,              // C
    message: Option<Range<usize>>,           // M
    detail: Option<Range<usize>>,            // D
    hint: Option<Range<usize>>,              // H
    position: Option<Range<usize>>,          // P
    internal_position: Option<Range<usize>>, // p
    internal_query: Option<Range<usize>>,    // q
    r#where: Option<Range<usize>>,           // W
    schema: Option<Range<usize>>,            // s
    table: Option<Range<usize>>,             // t
    column: Option<Range<usize>>,            // c
    datatype: Option<Range<usize>>,          // d
    constraint: Option<Range<usize>>,        // n
    file: Option<Range<usize>>,              // F
    line: Option<Range<usize>>,              // L
    routine: Option<Range<usize>>,           // R
}

impl std::fmt::Display for PgErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sev = self.local_severity().unwrap_or("UNKNOWN".into());
        let code = self.code().unwrap_or("?????".into());
        let msg = self.message().unwrap_or("<no message>".into());
        write!(f, "[{sev}] {code}: {msg}")
    }
}

impl std::fmt::Debug for PgErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgErrorResponse")
            .field("local_severity", &self.local_severity())
            .field("severity", &self.severity())
            .field("code", &self.code())
            .field("message", &self.message())
            .field("detail", &self.detail())
            .field("hint", &self.hint())
            .field("position", &self.position())
            .field("where", &self.r#where())
            .field("file", &self.file())
            .field("line", &self.line())
            .field("routine", &self.routine())
            .finish_non_exhaustive()
    }
}

impl PgErrorResponse {
    fn new(body: Bytes) -> Self {
        let mut resp = PgErrorResponse {
            body: body.clone(),
            local_severity: None,
            severity: None,
            code: None,
            message: None,
            detail: None,
            hint: None,
            position: None,
            internal_position: None,
            internal_query: None,
            r#where: None,
            schema: None,
            table: None,
            column: None,
            datatype: None,
            constraint: None,
            file: None,
            line: None,
            routine: None,
        };

        let mut offset = 0;
        let mut iter = body.split(|b| *b == 0);

        while let Some(field) = iter.next() {
            if field.is_empty() {
                break;
            }

            // field[0] = tag, field[1..] = value
            let tag = field[0];
            let start = offset + 1;
            let end = start + field.len() - 1; // minus tag

            let range = start..end;
            match tag {
                b'S' => resp.local_severity = Some(range),
                b'V' => resp.severity = Some(range),
                b'C' => resp.code = Some(range),
                b'M' => resp.message = Some(range),
                b'D' => resp.detail = Some(range),
                b'H' => resp.hint = Some(range),
                b'P' => resp.position = Some(range),
                b'p' => resp.internal_position = Some(range),
                b'q' => resp.internal_query = Some(range),
                b'W' => resp.r#where = Some(range),
                b's' => resp.schema = Some(range),
                b't' => resp.table = Some(range),
                b'c' => resp.column = Some(range),
                b'd' => resp.datatype = Some(range),
                b'n' => resp.constraint = Some(range),
                b'F' => resp.file = Some(range),
                b'L' => resp.line = Some(range),
                b'R' => resp.routine = Some(range),
                _ => {}
            }

            offset += field.len() + 1; // +1 for the null terminator
        }

        resp
    }
}

impl PgErrorResponse {
    fn field(&self, range: &Option<Range<usize>>) -> Option<Cow<'_, str>> {
        range
            .as_ref()
            .map(|r| String::from_utf8_lossy(&self.body[r.start..r.end]))
    }

    pub fn local_severity(&self) -> Option<Cow<'_, str>> {
        self.field(&self.local_severity)
    }

    pub fn severity(&self) -> Option<Cow<'_, str>> {
        self.field(&self.severity)
    }

    pub fn code(&self) -> Option<Cow<'_, str>> {
        self.field(&self.code)
    }

    pub fn message(&self) -> Option<Cow<'_, str>> {
        self.field(&self.message)
    }

    pub fn detail(&self) -> Option<Cow<'_, str>> {
        self.field(&self.detail)
    }

    pub fn hint(&self) -> Option<Cow<'_, str>> {
        self.field(&self.hint)
    }

    pub fn position(&self) -> Option<Cow<'_, str>> {
        self.field(&self.position)
    }

    pub fn internal_query(&self) -> Option<Cow<'_, str>> {
        self.field(&self.internal_query)
    }

    pub fn r#where(&self) -> Option<Cow<'_, str>> {
        self.field(&self.r#where)
    }

    pub fn file(&self) -> Option<Cow<'_, str>> {
        self.field(&self.file)
    }

    pub fn line(&self) -> Option<Cow<'_, str>> {
        self.field(&self.line)
    }

    pub fn routine(&self) -> Option<Cow<'_, str>> {
        self.field(&self.routine)
    }
}

impl TryFrom<PgFrame> for PgErrorResponse {
    type Error = PgFrame;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code == backend::MessageCode::ERROR_RESPONSE {
            Ok(PgErrorResponse::new(frame.body))
        } else {
            Err(frame)
        }
    }
}
