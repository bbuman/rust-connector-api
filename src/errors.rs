use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    /// ReqwestError.
    #[error("ReqwestError error")]
    ReqwestError,

    /// HTTP response error.
    #[error("HTTP error: `{0}`, `{1}`, {2}`")]
    HttpError(String, String, reqwest::StatusCode),

    /// Library error.
    #[error("Library error: `{0}`")]
    LibraryError(String),

    /// Polars error.
    #[error("Polars error")]
    PolarsError,

    /// Generic error.
    #[error(transparent)]
    GenericError(#[from] Box<dyn std::error::Error>),

    /// Parse error.
    #[error("Parsing error")]
    ParseError,
}


impl From<polars::error::PolarsError> for ConnectorError {
    fn from(_: polars::error::PolarsError) -> Self {
        ConnectorError::PolarsError
    }
}

impl From<url::ParseError> for ConnectorError {
    fn from(_: url::ParseError) -> Self {
        ConnectorError::ParseError
    }
}

impl From<reqwest::Error> for ConnectorError {
    fn from(_: reqwest::Error) -> Self {
        ConnectorError::ReqwestError
    }
}