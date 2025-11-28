use crate::transaction::{Client, Failure, Transaction, TransactionId};
use crate::wallet::Wallet;
use dashmap::DashMap;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;

pub struct WalletManager {
    wallets: DashMap<Client, Wallet>,
    transaction_journal: DashMap<Client, HashMap<TransactionId, Transaction>>, // For big sets would require a more memory efficient struct
}

impl WalletManager {
    pub fn init() -> Self {
        WalletManager {
            wallets: DashMap::new(),
            transaction_journal: DashMap::new(),
        }
    }

    pub async fn run(
        &self,
        mut tx_recv: UnboundedReceiver<Transaction>,
        err_send: UnboundedSender<Failure>,
    ) {
        while let Some(transaction) = tx_recv.recv().await {
            let res = match transaction {
                Transaction::Deposit {
                    client,
                    tx_id,
                    amount,
                } => {
                    self.wallets
                        .entry(client)
                        .or_insert_with(|| Wallet::new(client))
                        .deposit(tx_id, amount);
                    self.transaction_journal
                        .entry(client)
                        .or_insert_with(|| HashMap::new())
                        .insert(
                            tx_id,
                            Transaction::Deposit {
                                client,
                                tx_id,
                                amount,
                            },
                        );
                    Ok(())
                }
                Transaction::Withdrawal {
                    client,
                    tx_id,
                    amount,
                } => {
                    if let Some(mut wallet) = self.wallets.get_mut(&client) {
                        wallet.withdraw(tx_id, amount).and_then(|_| {
                            self.transaction_journal
                                .entry(client)
                                .or_insert_with(|| HashMap::new())
                                .insert(
                                    tx_id,
                                    Transaction::Withdrawal {
                                        client,
                                        tx_id,
                                        amount,
                                    },
                                );
                            Ok(())
                        })
                    } else {
                        Err(Failure::no_wallet(client, tx_id))
                    }
                }
                Transaction::Dispute { client, tx_id } => {
                    let tx = self
                        .transaction_journal
                        .get(&client)
                        .and_then(|txs| txs.get(&tx_id).cloned());

                    match tx {
                        Some(Transaction::Deposit { amount, .. }) => {
                            if let Some(mut wallet) = self.wallets.get_mut(&client) {
                                Ok(wallet.dispute(tx_id, amount))
                            } else {
                                Err(Failure::no_wallet(client, tx_id))
                            }
                        }
                        Some(Transaction::Withdrawal { .. }) => Err(Failure::new(
                            client,
                            tx_id,
                            "Can't dispute a withdraw!".to_string(),
                        )),
                        _ => Err(Failure::new(
                            client,
                            tx_id,
                            "Transaction to dispute was not found!".to_string(),
                        )),
                    }
                }
                Transaction::Resolve { client, tx_id } => {
                    if let Some(mut wallet) = self.wallets.get_mut(&client) {
                        wallet.settle_dispute(tx_id)
                    } else {
                        Err(Failure::no_wallet(client, tx_id))
                    }
                }
                Transaction::ChargeBack { client, tx_id } => {
                    if let Some(mut wallet) = self.wallets.get_mut(&client) {
                        wallet.charge_back(tx_id)
                    } else {
                        Err(Failure::no_wallet(client, tx_id))
                    }
                }
            };
            if let Err(e) = res {
                if err_send.send(e).is_err() {
                    break;
                }
            }
        }
    }

    pub fn export_wallets(&self) -> Vec<Wallet> {
        self.wallets.iter().map(|r| r.value().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::Amount;
    use crate::wallet::Balance;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_deposit_withdraw_transaction() {
        let wallet_manager = Arc::new(WalletManager::init());
        let (tx_sender, tx_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (err_sender, _err_receiver) = tokio::sync::mpsc::unbounded_channel();
        let wallet_manager_runner = tokio::spawn({
            let wallet_manager = wallet_manager.clone();
            async move { wallet_manager.run(tx_receiver, err_sender).await }
        });
        let client = Client::new(1);
        let deposit_amount = Amount::unsafe_new(100.0);
        let transactions = vec![
            Transaction::Deposit {
                client: client.clone(),
                tx_id: TransactionId::new(1),
                amount: deposit_amount.clone(),
            },
            Transaction::Withdrawal {
                client: client.clone(),
                tx_id: TransactionId::new(2),
                amount: deposit_amount.clone(),
            },
        ];
        for transaction in transactions {
            tx_sender.send(transaction).unwrap();
        }
        drop(tx_sender);
        wallet_manager_runner.await.unwrap();

        let wallets = wallet_manager.export_wallets();
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].client, client);
        assert_eq!(
            wallets[0].balance,
            Balance {
                available: Amount::zero(),
                held: Amount::zero(),
                total: Amount::zero(),
            }
        );
    }

    #[tokio::test]
    async fn test_dispute_resolve_transaction() {
        let wallet_manager = Arc::new(WalletManager::init());
        let (tx_sender, tx_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (err_sender, _err_receiver) = tokio::sync::mpsc::unbounded_channel();
        let wallet_manager_runner = tokio::spawn({
            let wallet_manager = wallet_manager.clone();
            async move { wallet_manager.run(tx_receiver, err_sender).await }
        });
        let client = Client::new(1);
        let deposit_amount = Amount::unsafe_new(100.0);
        tx_sender
            .send(Transaction::Deposit {
                client: client.clone(),
                tx_id: TransactionId::new(1),
                amount: deposit_amount.clone(),
            })
            .unwrap();
        tx_sender
            .send(Transaction::Dispute {
                client: client.clone(),
                tx_id: TransactionId::new(1),
            })
            .unwrap();
        tx_sender
            .send(Transaction::Resolve {
                client: client.clone(),
                tx_id: TransactionId::new(1),
            })
            .unwrap();
        drop(tx_sender);
        wallet_manager_runner.await.unwrap();

        let wallets = wallet_manager.export_wallets();
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].client, client);
        assert_eq!(
            wallets[0].balance,
            Balance {
                available: deposit_amount.clone(),
                held: Amount::zero(),
                total: deposit_amount.clone(),
            }
        );
    }

    #[tokio::test]
    async fn test_dispute_chargeback_transaction() {
        let wallet_manager = Arc::new(WalletManager::init());
        let (tx_sender, tx_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (err_sender, _err_receiver) = tokio::sync::mpsc::unbounded_channel();
        let wallet_manager_runner = tokio::spawn({
            let wallet_manager = wallet_manager.clone();
            async move { wallet_manager.run(tx_receiver, err_sender).await }
        });
        let client = Client::new(1);
        let deposit_amount = Amount::unsafe_new(100.0);
        tx_sender
            .send(Transaction::Deposit {
                client: client.clone(),
                tx_id: TransactionId::new(1),
                amount: deposit_amount.clone(),
            })
            .unwrap();
        tx_sender
            .send(Transaction::Dispute {
                client: client.clone(),
                tx_id: TransactionId::new(1),
            })
            .unwrap();
        tx_sender
            .send(Transaction::ChargeBack {
                client: client.clone(),
                tx_id: TransactionId::new(1),
            })
            .unwrap();
        drop(tx_sender);
        wallet_manager_runner.await.unwrap();

        let wallets = wallet_manager.export_wallets();
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].client, client);
        assert_eq!(wallets[0].locked, true);
        assert_eq!(
            wallets[0].balance,
            Balance {
                available: Amount::zero(),
                held: Amount::zero(),
                total: Amount::zero(),
            }
        );
    }
}
