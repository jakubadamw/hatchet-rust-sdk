use hatchet_sdk::{Client, Context, StepBuilder, WorkflowBuilder};

#[derive(serde::Serialize, serde::Deserialize)]
struct HelloOutput {
    text: String,
}

async fn execute_hello(_context: Context, _: serde_json::Value) -> anyhow::Result<HelloOutput> {
    Ok(HelloOutput {
        text: "Hello".to_owned(),
    })
}

async fn execute_world(
    mut context: Context,
    _: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let hello_result: HelloOutput = context.pop_parent_output("hello");
    Ok(serde_json::json!({
        "text": format!("{} World!", hello_result.text)
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
            .name("hello-world")
            .step(
                StepBuilder::default()
                    .name("hello")
                    .function(&execute_hello)
                    .build()?,
            )
            .step(
                StepBuilder::default()
                    .name("world")
                    .function(&execute_world)
                    .parent("hello")
                    .build()?,
            )
            .build()?,
    );
    worker.start().await?;
    Ok(())
}
