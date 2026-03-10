#[tokio::main]
async fn main() -> anyhow::Result<()> {
    crawfish_cli::run_cli().await
}
