use crate::transaction::Transaction;
use crate::wallet::Wallet;
use crate::wallet_manager::WalletManager;
use csv::Writer;
use log::info;
use std::sync::Arc;
use std::{env, io};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;

mod transaction;
mod wallet;
mod wallet_manager;

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -- <input.csv>");
        std::process::exit(1);
    }
    let wallet_manager = Arc::new(WalletManager::init());
    let (tx_sender, tx_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (err_sender, mut err_receiver) = tokio::sync::mpsc::unbounded_channel();
    let wallet_manager_runner = tokio::spawn({
        let wallet_manager = wallet_manager.clone();
        async move { wallet_manager.run(tx_receiver, err_sender).await }
    });

    stream_csv_into_channel(args[1].clone(), tx_sender).await?;

    let _error_runner = tokio::spawn(async move {
        while let Some(failure) = err_receiver.recv().await {
            info!("Transaction failed: {:?}", failure); // Would handle failure. Maybe send notification to customer..
        }
    });

    wallet_manager_runner.await?;
    let wallets = wallet_manager.export_wallets();
    write_wallets_csv(wallets.as_slice())?;
    Ok(())
}

pub fn write_wallets_csv(wallets: &[Wallet]) -> csv::Result<()> {
    let mut wtr = Writer::from_writer(io::stdout());
    for wallet in wallets {
        wtr.serialize(wallet)?;
    }
    wtr.flush()?;
    Ok(())
}

pub async fn stream_csv_into_channel(
    path: String,
    tx_sender: UnboundedSender<Transaction>,
) -> anyhow::Result<()> {
    task::spawn_blocking(move || {
        let mut csv_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(path)?;

        for csv_row in csv_reader.records() {
            let csv_row = csv_row?;
            if let Some(tx) = Transaction::from_csv_row(&csv_row) {
                tx_sender
                    .send(tx)
                    .expect("Failed to send transaction through channel")
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await??;

    Ok(())
}
