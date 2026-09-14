use std::fmt;

#[derive(Debug)]
pub enum Error {
    Database(rusqlite::Error),
    Io(std::io::Error),
    Torrent(String),
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(e) => write!(f, "database error: {e}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Torrent(e) => write!(f, "torrent error: {e}"),
            Error::NotFound(e) => write!(f, "not found: {e}"),
            Error::BadRequest(e) => write!(f, "bad request: {e}"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Database(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        let (status, msg) = match &self {
            Error::NotFound(e) => (StatusCode::NOT_FOUND, e.clone()),
            Error::BadRequest(e) => (StatusCode::BAD_REQUEST, e.clone()),
            Error::Torrent(e) => (StatusCode::BAD_GATEWAY, e.clone()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        (status, msg).into_response()
    }
}
