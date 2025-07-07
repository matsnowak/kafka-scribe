pub mod commands;

use clap::Parser;

#[derive(Parser)]
#[command(name = "kscribe")]
#[command(about = "A CLI tool for Kafka message capture, analysis, and replay")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::Subcommand)]
pub enum Commands {
    /// Store messages from a Kafka topic
    Store(commands::store::StoreCommand),
    /// Replay messages to a Kafka topic
    Replay(commands::replay::ReplayCommand),
    /// Show statistics about stored messages
    Stats(commands::stats::StatsCommand),
    /// Generate shell completion scripts
    Completion(commands::completion::CompletionCommand),
}