use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    PgMessage,
    message::backend::{Authentication, MessageReader},
    startup,
};

/// Reads the next authentication-related message, skipping notices and mapping
/// an `ErrorResponse` to [`startup::Error::Server`].
pub(crate) async fn read_auth_message<S>(
    stream: &mut S,
    reader: &mut MessageReader,
) -> startup::Result<Authentication>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match reader.read_message(stream).await? {
            PgMessage::ErrorResponse(pg_err) => {
                return Err(startup::Error::Server(pg_err));
            }
            // Notices may precede the authentication request; skip them.
            PgMessage::NoticeResponse(_) => continue,
            PgMessage::Authentication(auth) => return Ok(auth),
            msg => return Err(format!("unexpected message: {msg:?}"))?,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    fn backend_message(code: u8, body: &[u8]) -> Vec<u8> {
        let mut message = Vec::with_capacity(body.len() + 5);
        message.push(code);
        message.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        message.extend_from_slice(body);
        message
    }

    #[tokio::test]
    async fn long_notice_stream_is_processed_iteratively() {
        const NOTICE_COUNT: usize = 4_096;

        let (mut client, mut server) = tokio::io::duplex(1_024);
        let writer = tokio::spawn(async move {
            let notice = backend_message(
                b'N',
                b"SNOTICE\0VNOTICE\0C00000\0Mauthentication notice\0\0",
            );
            for _ in 0..NOTICE_COUNT {
                server.write_all(&notice).await.unwrap();
            }
            server
                .write_all(&backend_message(b'R', &0u32.to_be_bytes()))
                .await
                .unwrap();
        });

        let mut reader = MessageReader::new();
        assert!(matches!(
            read_auth_message(&mut client, &mut reader).await.unwrap(),
            Authentication::Ok
        ));
        writer.await.unwrap();
    }
}
