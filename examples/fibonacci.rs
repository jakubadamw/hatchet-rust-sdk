use hatchet_sdk::{Client, StepBuilder, WorkflowBuilder};

fn fibonacci(n: u32) -> u32 {
    (1..=n)
        .fold((0, 1), |(last, current), _| (current, last + current))
        .0
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    #[rstest]
    #[case(0, 0)]
    #[case(1, 1)]
    #[case(2, 1)]
    #[case(3, 2)]
    #[case(4, 3)]
    #[case(5, 5)]
    #[case(6, 8)]
    fn fibonacci_test(#[case] input: u32, #[case] expected: u32) {
        assert_eq!(expected, super::fibonacci(input))
    }
}

#[derive(serde::Deserialize)]
struct Input {
    n: u32,
}

#[derive(serde::Serialize)]
struct Output {
    result: u32,
}

async fn execute(Input { n }: Input) -> anyhow::Result<Output> {
    Ok(Output {
        result: fibonacci(n),
    })
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
    let mut worker = client.worker("example_fibonacci").build();
    worker.register_workflow(
        WorkflowBuilder::default()
            .name("fibonacci")
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
