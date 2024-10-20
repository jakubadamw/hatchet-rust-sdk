use secrecy::SecretString;

use crate::worker::WorkerBuilder;

#[derive(serde::Deserialize, Debug)]
pub(crate) struct Environment {
    pub(crate) token: SecretString,
    pub(crate) host_port: Option<String>,
    pub(crate) listener_v2_timeout: Option<u64>,
    #[serde(default)]
    pub(crate) tls_strategy: crate::ClientTlStrategy,
    pub(crate) tls_cert_file: Option<String>,
    pub(crate) tls_cert: Option<String>,
    pub(crate) tls_key_file: Option<String>,
    pub(crate) tls_key: Option<String>,
    pub(crate) tls_root_ca_file: Option<String>,
    pub(crate) tls_root_ca: Option<String>,
    pub(crate) tls_server_name: Option<String>,
    #[serde(default)]
    pub(crate) namespace: String,
}

pub struct Client {
    environment: Environment,
}

impl Client {
    pub fn new() -> crate::InternalResult<Self> {
        let environment = envy::prefixed("HATCHET_CLIENT_").from_env::<Environment>()?;
        Ok(Self { environment })
    }

    pub fn worker(&self, worker_name: &str) -> WorkerBuilder {
        WorkerBuilder::default()
            .name(worker_name.to_string())
            .environment(&self.environment)
    }
}
