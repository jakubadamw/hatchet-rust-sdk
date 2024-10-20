#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to load configuration from the environment: {0}")]
    Environment(#[from] envy::Error),
    #[error("worker registration request: {0}")]
    CouldNotRegisterWorker(tonic::Status),
    #[error("workflow registration request:: {0}")]
    CouldNotPutWorkflow(tonic::Status),
    #[error("dispatcher listen error: {0}")]
    CouldNotListenToDispatcher(tonic::Status),
    #[error("step status send error: {0}")]
    CouldNotSendStepStatus(tonic::Status),
    #[error("heartbeat error: {0}")]
    CouldNotSendHeartbeat(tonic::Status),
    #[error("dispatcher connection error: {0}")]
    CouldNotConnectToDispatcher(tonic::transport::Error),
    #[error("workflow service connection error: {0:?}")]
    CouldNotConnectToWorkflowService(tonic::transport::Error),
    #[error("could not read file under `{1}`: {0}")]
    CouldNotReadFile(std::io::Error, String),
    #[error("environment variables {0} and {1} cannot be set simultaneously")]
    CantSetBothEnvironmentVariables(&'static str, &'static str),
    #[error("could not subscribe to actions after {0} retries")]
    CouldNotSubscribeToActions(usize),
    #[error("could not decode the provided token to retrieve the host/port pair: {0}")]
    CouldNotDecodeToken(jsonwebtoken::errors::Error),
    #[error("could not decode action payload: {0}")]
    CouldNotDecodeActionPayload(serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
