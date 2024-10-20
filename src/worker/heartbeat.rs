const HEARTBEAT_DURATION: std::time::Duration = std::time::Duration::from_secs(4);

use crate::worker::grpc::HeartbeatRequest;

use super::grpc::dispatcher_client::DispatcherClient;

pub(crate) async fn run<F>(
    mut dispatcher: DispatcherClient<
        tonic::service::interceptor::InterceptedService<tonic::transport::Channel, F>,
    >,
    worker_id: &str,
    mut interrupt_receiver: tokio::sync::mpsc::Receiver<()>,
) -> crate::InternalResult<()>
where
    F: tonic::service::Interceptor + Send + 'static,
{
    let worker_id = worker_id.to_owned();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_DURATION);
        loop {
            dispatcher
                .heartbeat(HeartbeatRequest {
                    heartbeat_at: Some(std::time::SystemTime::now().into()),
                    worker_id: worker_id.clone(),
                })
                .await
                .map_err(crate::InternalError::CouldNotSendHeartbeat)?;

            tokio::select! {
                _ = interval.tick() => {
                    continue;
                }
                _ = interrupt_receiver.recv() => {
                    break;
                }
            }
        }
        crate::InternalResult::Ok(())
    })
    .await
    .expect("must succeed spawing")?;

    Ok(())
}
