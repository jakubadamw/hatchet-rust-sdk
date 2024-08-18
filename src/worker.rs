use futures_util::StreamExt;
use grpc::{
    dispatcher_client::DispatcherClient, HeartbeatRequest, WorkerListenRequest,
    WorkerRegisterRequest, WorkerRegisterResponse,
};
use secrecy::ExposeSecret;
use tonic::transport::Certificate;

use crate::client::Environment;

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Worker<'a> {
    name: String,
    max_runs: Option<i32>,
    #[builder(vis = "pub(crate)")]
    environment: &'a super::client::Environment,
}

pub mod grpc {
    tonic::include_proto!("dispatcher");
}

const HEARTBEAT_DURATION: std::time::Duration = std::time::Duration::from_secs(4);

enum ListenStrategy {
    V1,
    V2,
}

const DEFAULT_ACTION_LISTENER_RETRY_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5);
const DEFAULT_ACTION_LISTENER_RETRY_COUNT: usize = 15;
const DEFAULT_ACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const DEFAULT_REGISTER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(serde::Deserialize)]
struct TokenClaims {
    grpc_broadcast_address: String,
}

fn construct_endpoint_url<'a>(
    host_port_in_environment: Option<&'a str>,
    token: &'a str,
) -> crate::Result<String> {
    Ok(format!(
        "https://{}",
        host_port_in_environment
            .map(|value| crate::Result::Ok(std::borrow::Cow::Borrowed(value)))
            .unwrap_or_else(|| {
                let key = jsonwebtoken::DecodingKey::from_secret(&[]);
                let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
                validation.insecure_disable_signature_validation();
                let data: jsonwebtoken::TokenData<TokenClaims> =
                    jsonwebtoken::decode(&token, &key, &validation)?;
                Ok(data.claims.grpc_broadcast_address.into())
            })?
    ))
}

impl<'a> Worker<'a> {
    pub(crate) async fn register_workflow(&self, workflow: crate::workflow::Workflow) {}

    async fn run_listener<F>(
        &self,
        mut dispatcher: DispatcherClient<
            tonic::service::interceptor::InterceptedService<tonic::transport::Channel, F>,
        >,
        worker_id: &str,
        mut interrupt_receiver: tokio::sync::broadcast::Receiver<()>,
    ) -> crate::Result<()>
    where
        F: tonic::service::Interceptor + Send + 'static,
    {
        let mut retries: usize = 0;
        let mut listen_strategy = ListenStrategy::V2;

        let mut connection_attempt = tokio::time::Instant::now();

        'main_loop: loop {
            if connection_attempt.elapsed() > DEFAULT_ACTION_LISTENER_RETRY_INTERVAL {
                retries = 0;
            }
            if retries > DEFAULT_ACTION_LISTENER_RETRY_COUNT {
                return Err(crate::Error::CouldNotSubscribeToActions(
                    DEFAULT_ACTION_LISTENER_RETRY_COUNT,
                ));
            }

            let mut stream = match listen_strategy {
                ListenStrategy::V1 => dispatcher
                    .listen(WorkerListenRequest {
                        worker_id: worker_id.to_owned(),
                    })
                    .await?
                    .into_inner(),
                ListenStrategy::V2 => dispatcher
                    .listen_v2(WorkerListenRequest {
                        worker_id: worker_id.to_owned(),
                    })
                    .await?
                    .into_inner(),
            };

            loop {
                tokio::select! {
                    element = stream.next() => {
                        let Some(result) = element else {
                            break 'main_loop;
                        };

                        let action = match result {
                            Err(status) => match status.code() {
                                tonic::Code::Cancelled => {
                                    return Ok(());
                                }
                                tonic::Code::DeadlineExceeded => {
                                    continue 'main_loop;
                                }
                                tonic::Code::Unimplemented => {
                                    listen_strategy = ListenStrategy::V1;
                                    continue 'main_loop;
                                }
                                _other => {
                                    retries += 1;
                                    continue 'main_loop;
                                }
                            },
                            Ok(action) => action,
                        };


                    }
                    _ = interrupt_receiver.recv() => {
                        break 'main_loop;
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_heartbeat<F>(
        &self,
        mut dispatcher: DispatcherClient<
            tonic::service::interceptor::InterceptedService<tonic::transport::Channel, F>,
        >,
        worker_id: &str,
        mut interrupt_receiver: tokio::sync::broadcast::Receiver<()>,
    ) -> crate::Result<()>
    where
        F: tonic::service::Interceptor + Send + 'static,
    {
        let worker_id = worker_id.to_owned();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_DURATION);
            loop {
                dispatcher
                    .heartbeat(HeartbeatRequest {
                        heartbeat_at: Some(
                            std::time::SystemTime::now()
                                .try_into()
                                .expect("must succeed"),
                        ),
                        worker_id: worker_id.clone(),
                    })
                    .await?;

                tokio::select! {
                    _ = interval.tick() => {
                        continue;
                    }
                    _ = interrupt_receiver.recv() => {
                        break;
                    }
                }
            }
            crate::Result::Ok(())
        })
        .await
        .expect("must succeed spawing")?;
        Ok(())
    }

    pub async fn start(&self) -> crate::Result<()> {
        use tonic::IntoRequest;

        let (interrupt_sender, interrupt_receiver1) = tokio::sync::broadcast::channel(1);
        let interrupt_receiver2 = interrupt_sender.subscribe();
        ctrlc_async::set_async_handler(async move {
            // Ignore the error.
            let _ = interrupt_sender.send(());
        })
        .expect("Error setting Ctrl-C handler");

        let Environment {
            token,
            host_port,
            tls_strategy,
            tls_cert_file,
            tls_cert,
            tls_key_file,
            tls_key,
            tls_root_ca_file,
            tls_root_ca,
            tls_server_name,
        } = self.environment;

        // FIXME: Account for all the settings from `self.environment`.
        let mut dispatcher = {
            let mut endpoint = tonic::transport::Endpoint::new(construct_endpoint_url(
                host_port.as_deref(),
                token.expose_secret(),
            )?)?;
            endpoint = match tls_strategy {
                crate::ClientTlStrategy::None => todo!(),
                crate::ClientTlStrategy::Tls | crate::ClientTlStrategy::MTls => {
                    let mut tls_config = tonic::transport::channel::ClientTlsConfig::new();
                    if let Some(domain_name) = tls_server_name.as_deref() {
                        tls_config = tls_config.domain_name(domain_name);
                    };
                    let extra_root_certificate =
                        match (tls_root_ca.as_deref(), tls_root_ca_file.as_deref()) {
                            (Some(_), Some(_)) => {
                                return Err(crate::Error::CantSetBothEnvironmentVariables(
                                    "HATCHET_CLIENT_TLS_ROOT_CA",
                                    "HATCHET_CLIENT_TLS_ROOT_CA_FILE",
                                ));
                            }
                            (Some(tls_root_ca), None) => {
                                Some(std::borrow::Cow::Borrowed(tls_root_ca.as_bytes()))
                            }
                            (None, Some(tls_root_ca_file)) => Some(std::borrow::Cow::Owned(
                                tokio::fs::read(tls_root_ca_file).await.map_err(|err| {
                                    crate::Error::CouldNotReadFile(err, tls_root_ca_file.to_owned())
                                })?,
                            )),
                            (None, None) => None,
                        };
                    if let Some(extra_root_certificate) = extra_root_certificate {
                        tls_config = tls_config
                            .ca_certificate(Certificate::from_pem(extra_root_certificate));
                    }
                    endpoint.tls_config(tls_config)?
                }
            };
            let connection = endpoint.connect().await?;
            let authorization_hedaer: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {token}", token = token.expose_secret())
                    .parse()
                    .expect("must parse successfully");

            grpc::dispatcher_client::DispatcherClient::with_interceptor(
                connection,
                move |mut request: tonic::Request<()>| {
                    request
                        .metadata_mut()
                        .insert("authorization", authorization_hedaer.clone());
                    Ok(request)
                },
            )
        };

        let request = {
            let mut request: tonic::Request<WorkerRegisterRequest> = WorkerRegisterRequest {
                worker_name: self.name.clone(),
                max_runs: self.max_runs,
                // FIXME: Implement.
                ..Default::default()
            }
            .into_request();
            request.set_timeout(DEFAULT_REGISTER_TIMEOUT);
            request
        };

        let WorkerRegisterResponse { worker_id, .. } =
            dispatcher.register(request).await?.into_inner();

        futures_util::try_join! {
            self.run_heartbeat(dispatcher.clone(), &worker_id, interrupt_receiver1),
            self.run_listener(dispatcher, &worker_id, interrupt_receiver2),
        }?;
        Ok(())
    }
}
