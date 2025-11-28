use crate::transaction::{Amount, Client, Failure, TransactionId};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Balance {
    pub available: Amount,
    pub held: Amount,
    pub total: Amount,
}

impl Balance {
    fn new() -> Self {
        Balance {
            available: Amount::unsafe_new(0.0),
            held: Amount::unsafe_new(0.0),
            total: Amount::unsafe_new(0.0),
        }
    }
}

#[derive(Clone)]
pub struct Wallet {
    pub(super) client: Client,
    pub(super) balance: Balance,
    pub(super) locked: bool,
    pub(super) open_disputes: HashMap<TransactionId, Amount>,
}

impl Wallet {
    pub fn new(client: Client) -> Self {
        Wallet {
            client,
            balance: Balance::new(),
            locked: false,
            open_disputes: HashMap::new(),
        }
    }

    pub fn dispute(&mut self, tx: TransactionId, amount: Amount) {
        self.balance.available -= amount;
        self.balance.held += amount;
        self.open_disputes.insert(tx, amount);
    }

    pub fn deposit(&mut self, _tx: TransactionId, amount: Amount) {
        self.balance.available += amount;
        self.balance.total += amount;
    }

    pub fn settle_dispute(&mut self, tx: TransactionId) -> Result<(), Failure> {
        if let Some(disputed_amount) = self.open_disputes.get(&tx) {
            self.balance.held -= *disputed_amount;
            self.balance.available += *disputed_amount;
            Ok(())
        } else {
            Err(Failure::new(
                self.client,
                tx,
                "Disputed transaction not found for settlement!".to_string(),
            ))
        }
    }

    pub fn charge_back(&mut self, tx: TransactionId) -> Result<(), Failure> {
        if let Some(disputed_amount) = self.open_disputes.get(&tx) {
            self.balance.held -= *disputed_amount;
            self.balance.total -= *disputed_amount;
            self.locked = true;
            Ok(())
        } else {
            Err(Failure::new(
                self.client,
                tx,
                "Disputed transaction not found for charge back!".to_string(),
            ))
        }
    }

    pub fn withdraw(&mut self, tx: TransactionId, amount: Amount) -> Result<(), Failure> {
        if self.balance.available >= amount {
            self.balance.available -= amount;
            self.balance.total -= amount;
            Ok(())
        } else {
            Err(Failure::insufficient_funds(self.client, tx))
        }
    }
}

impl Serialize for Wallet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Wallet", 5)?;
        s.serialize_field("client", &self.client)?;
        s.serialize_field("available", &self.balance.available)?;
        s.serialize_field("held", &self.balance.held)?;
        s.serialize_field("total", &self.balance.total)?;
        s.serialize_field("locked", &self.locked)?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_deposit() {
        let client = Client::new(1);
        let mut wallet = Wallet::new(client);
        let tx_id = TransactionId::new(1001);
        let amount = Amount::unsafe_new(150.0);

        wallet.deposit(tx_id, amount);

        assert_eq!(wallet.balance.available, amount);
        assert_eq!(wallet.balance.total, amount);
    }

    #[test]
    fn test_wallet_withdraw() {
        let client = Client::new(1);
        let mut wallet = Wallet::new(client);
        let tx_id = TransactionId::new(1001);
        let deposit_amount = Amount::unsafe_new(200.0);
        let withdraw_amount = Amount::unsafe_new(50.0);

        wallet.deposit(tx_id, deposit_amount);
        let result = wallet.withdraw(tx_id, withdraw_amount);

        assert!(result.is_ok());
        assert_eq!(wallet.balance.available, Amount::unsafe_new(150.0));
        assert_eq!(wallet.balance.total, deposit_amount - withdraw_amount);
    }

    #[test]
    fn test_wallet_dispute_and_settle() {
        let client = Client::new(1);
        let mut wallet = Wallet::new(client);
        let tx_id = TransactionId::new(1001);
        let deposit_amount = Amount::unsafe_new(300.0);
        let dispute_amount = Amount::unsafe_new(100.0);

        wallet.deposit(tx_id, deposit_amount);
        wallet.dispute(tx_id, dispute_amount);

        assert_eq!(wallet.balance.available, Amount::unsafe_new(200.0));
        assert_eq!(wallet.balance.held, dispute_amount);

        let settle_result = wallet.settle_dispute(tx_id);
        assert!(settle_result.is_ok());
        assert_eq!(wallet.balance.available, Amount::unsafe_new(300.0));
        assert_eq!(wallet.balance.held, Amount::zero());
    }

    #[test]
    fn test_wallet_charge_back() {
        let client = Client::new(1);
        let mut wallet = Wallet::new(client);
        let tx_id = TransactionId::new(1001);
        let deposit_amount = Amount::unsafe_new(400.0);
        let dispute_amount = Amount::unsafe_new(150.0);

        wallet.deposit(tx_id, deposit_amount);
        wallet.dispute(tx_id, dispute_amount);

        assert_eq!(wallet.balance.available, Amount::unsafe_new(250.0));
        assert_eq!(wallet.balance.held, dispute_amount);

        let charge_back_result = wallet.charge_back(tx_id);
        assert!(charge_back_result.is_ok());
        assert_eq!(wallet.balance.total, Amount::unsafe_new(250.0));
        assert_eq!(wallet.balance.held, Amount::zero());
        assert!(wallet.locked);
    }
}
