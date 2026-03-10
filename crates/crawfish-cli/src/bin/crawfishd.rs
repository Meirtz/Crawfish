use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "crawfishd", version, about = "Crawfish supervisor daemon")]
struct Args {
    #[arg(long, default_value = "Crawfish.toml")]
    config: PathBuf,
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    crawfish_cli::run_daemon(args.config, args.once).await
}
