mod cli;
mod core;
mod formats;
mod kafka;
mod plugins;
mod storage;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use tracing::debug;
use tracing::field::debug;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()?;
    tracing::debug!("Starting kafka-scribe debug");

    // Parse command line arguments
    let cli = Cli::parse();

    // Execute the appropriate command
    match cli.command {
        Commands::Store(cmd) => {
            tracing::info!("Executing store command");
            cmd.execute().await?;
        }
        Commands::Replay(cmd) => {
            tracing::info!("Executing replay command");
            cmd.execute().await?;
        }
        Commands::Stats(cmd) => {
            tracing::info!("Executing stats command");
            cmd.execute().await?;
        }
        Commands::Completion(cmd) => {
            tracing::info!("Executing completion command");
            cmd.execute()?;
        }
    }

    Ok(())
}
