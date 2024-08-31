#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to load configuration from the environment: {0}")]
    Environment(#[from] envy::Error),
    #[error("transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("could not read file under `{1}`: {0}")]
    CouldNotReadFile(std::io::Error, String),
    #[error("environment variables {0} and {1} cannot be set simultaneously")]
    CantSetBothEnvironmentVariables(&'static str, &'static str),
    #[error("could not subscribe to actions after {0} retries")]
    CouldNotSubscribeToActions(usize),
    #[error("could not decode the provided token to retrieve the host/port pair")]
    CouldNotDecodeToken(jsonwebtoken::errors::Error),
    #[error("could not decode action payload: {0}")]
    CouldNotDecodeActionPayload(serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
