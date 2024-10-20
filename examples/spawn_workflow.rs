use hatchet_sdk::{Client, Context, StepBuilder, WorkflowBuilder};

async fn execute_hello(
    context: Context,
    _: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    context
        .trigger_workflow(
            "world",
            serde_json::json!({
                "x": 42
            }),
        )
        .await?;
    Ok(serde_json::json!({
        "message": "Hello"
    }))
}

async fn execute_world(
    _context: Context,
    _: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::json!({
        "message": "World"
    }))
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
    let mut worker = client.worker("example_spawn_workflow").build();
    worker.register_workflow(
        WorkflowBuilder::default()
            .name("hello")
            .step(
                StepBuilder::default()
                    .name("execute")
                    .function(&execute_hello)
                    .build()?,
            )
            .build()?,
    );
    worker.register_workflow(
        WorkflowBuilder::default()
            .name("world")
            .step(
                StepBuilder::default()
                    .name("execute")
                    .function(&execute_world)
                    .build()?,
            )
            .build()?,
    );
    worker.start().await?;
    Ok(())
}
