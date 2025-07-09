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
use tracing::Level;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

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
