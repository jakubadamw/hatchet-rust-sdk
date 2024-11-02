use hatchet_sdk::{Client, Context, StepBuilder, WorkflowBuilder};

#[derive(serde::Deserialize)]
struct Input {}

async fn execute(context: Context, _input: Input) -> anyhow::Result<()> {
    assert_eq!(context.datum::<Datum>().number, 10);
    Ok(())
}

struct Datum {
    number: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("hatchet_sdk=debug".parse()?),
        )
        .init();

    let client = Client::new()?;
    let mut worker = client
        .worker("example_data_in_context")
        .datum(Datum { number: 10 })
        .build();
    worker.register_workflow(
        WorkflowBuilder::default()
            .name("example_data_in_context")
            .step(
                StepBuilder::default()
                    .name("compute")
                    .function(&execute)
                    .build()?,
            )
            .build()?,
    );
    worker.start().await?;
    Ok(())
}
