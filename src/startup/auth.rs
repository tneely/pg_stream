use tokio::io::{AsyncRead, AsyncWrite};

use crate::{startup, messages::backend};

pub(crate) enum AuthMessage {
    Ok,
    KerberosV5,
    CleartextPassword,
    Md5Password([u8; 4]),
    Gss,
    GssContinue,
    Sspi,
    Sasl(AuthMechanism),
    SaslContinue(String),
    SaslFinal(String),
}

impl std::fmt::Display for AuthMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMessage::Ok => write!(f, "AuthenticationOk"),
            AuthMessage::KerberosV5 => write!(f, "AuthenticationKerberosV5"),
            AuthMessage::CleartextPassword => write!(f, "AuthenticationCleartextPassword"),
            AuthMessage::Md5Password(_salt) => write!(f, "AuthenticationMD5Password"),
            AuthMessage::Gss => write!(f, "AuthenticationGSS"),
            AuthMessage::GssContinue => write!(f, "AuthenticationGSSContinue"),
            AuthMessage::Sspi => write!(f, "AuthenticationSSPI"),
            AuthMessage::Sasl(mech) => write!(f, "AuthenticationSASL({mech})"),
            AuthMessage::SaslContinue(_) => write!(f, "AuthenticationSASLContinue"),
            AuthMessage::SaslFinal(_) => write!(f, "AuthenticationSASLContinue"),
        }
    }
}

pub(crate) enum AuthMechanism {
    ScramSha256,
    // ScramSha256Plus,
    // OAuthBearer,
}

impl std::fmt::Display for AuthMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ScramSha256 => "SCRAM-SHA-256",
            // Self::ScramSha256Plus => "SCRAM-SHA-256-PLUS",
            // Self::OAuthBearer => "OAUTHBEARER",
        };
        write!(f, "{name}")
    }
}

impl TryFrom<&str> for AuthMechanism {
    type Error = startup::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "SCRAM-SHA-256" => Ok(AuthMechanism::ScramSha256),
            // "SCRAM-SHA-256-PLUS" => Ok(AuthMechanism::ScramSha256Plus),
            // "OAUTHBEARER" => Ok(AuthMechanism::OAuthBearer),
            _ => Err(format!("unsupported authentication mechanism {value}"))?,
        }
    }
}

pub(crate) async fn read_auth_message<S>(stream: &mut S) -> startup::Result<AuthMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let msg = backend::read_frame(stream).await?;

    match msg.code {
        backend::MessageCode::ERROR_RESPONSE => {
            let pg_err = msg.try_into().expect("is an error response");
            Err(startup::Error::Server(pg_err))
        }
        backend::MessageCode::AUTHENTICATION => {
            let auth_code = u32::from_be_bytes(msg.body[..4].try_into().unwrap());
            let msg = match auth_code {
                0 => AuthMessage::Ok,
                2 => AuthMessage::KerberosV5,
                3 => AuthMessage::CleartextPassword,
                5 => {
                    let salt = msg.body[4..]
                        .try_into()
                        .map_err(|_| "unexpected body length in md5 password challenge")?;
                    AuthMessage::Md5Password(salt)
                }
                7 => AuthMessage::Gss,
                8 => AuthMessage::GssContinue,
                9 => AuthMessage::Sspi,
                10 => {
                    let mech = msg.body[4..]
                        .split(|b| *b == 0)
                        .map(String::from_utf8_lossy)
                        .find_map(|m| AuthMechanism::try_from(m.as_ref()).ok())
                        .ok_or("no supported authentication mechanisms")?;
                    AuthMessage::Sasl(mech)
                }
                11 => {
                    let resp = &msg.body[4..];
                    AuthMessage::SaslContinue(String::from_utf8_lossy(resp).to_string())
                }
                12 => {
                    let resp = &msg.body[4..];
                    AuthMessage::SaslFinal(String::from_utf8_lossy(resp).to_string())
                }
                auth_code => Err(format!("unexpected auth response code {auth_code}",))?,
            };
            Ok(msg)
        }
        code => Err(format!("unexpected message code {code}"))?,
    }
}
