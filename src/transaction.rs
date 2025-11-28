use csv::StringRecord;
use serde::{Deserialize, Serialize, Serializer};
use std::iter::Sum;
use std::ops::{Add, AddAssign, Sub, SubAssign};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Transaction {
    Deposit {
        client: Client,
        tx_id: TransactionId,
        amount: Amount,
    },
    Withdrawal {
        client: Client,
        tx_id: TransactionId,
        amount: Amount,
    },
    Dispute {
        client: Client,
        tx_id: TransactionId,
    },
    Resolve {
        client: Client,
        tx_id: TransactionId,
    },
    ChargeBack {
        client: Client,
        tx_id: TransactionId,
    },
}

impl Transaction {
    pub fn from_csv_row(csv_row: &StringRecord) -> Option<Transaction> {
        let transaction_type = csv_row.get(0)?;
        let client: u16 = csv_row.get(1).and_then(|s| s.parse().ok())?;
        let tx: u32 = csv_row.get(2).and_then(|s| s.parse().ok())?;
        let amount: Option<f32> = csv_row.get(3).and_then(|s| s.parse().ok());

        let tx_id = TransactionId(tx);
        let client = Client(client);

        match transaction_type {
            "deposit" => Some(Transaction::Deposit {
                client,
                tx_id,
                amount: amount.and_then(|a| Amount::try_from(a).ok())?,
            }),
            "withdrawal" => Some(Transaction::Withdrawal {
                client,
                tx_id,
                amount: amount.and_then(|a| Amount::try_from(a).ok())?,
            }),
            "dispute" => Some(Transaction::Dispute { client, tx_id }),
            "resolve" => Some(Transaction::Resolve { client, tx_id }),
            "chargeback" => Some(Transaction::ChargeBack { client, tx_id }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Amount(f32);

impl Amount {
    pub fn unsafe_new(value: f32) -> Self {
        Amount(value)
    }

    pub fn zero() -> Self {
        Amount(0.0)
    }
}

impl TryFrom<f32> for Amount {
    type Error = String;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        if value >= 0.0 {
            Ok(Amount(value))
        } else {
            Err("Amount must be positive".to_string())
        }
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        let value: f32 = s.parse().map_err(serde::de::Error::custom)?;
        Amount::try_from(value).map_err(|e| serde::de::Error::custom(e))
    }
}

impl Serialize for Amount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{:.4}", self.0);
        serializer.serialize_str(s.as_str())
    }
}

impl Add for Amount {
    type Output = Amount;

    fn add(self, other: Self) -> Self::Output {
        Amount(self.0 + other.0)
    }
}

impl Sum for Amount {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        Amount(iter.map(|t| t.0).sum())
    }
}

impl AddAssign for Amount {
    fn add_assign(&mut self, another: Self) {
        self.0 += another.0;
    }
}

impl SubAssign for Amount {
    fn sub_assign(&mut self, another: Self) {
        self.0 -= another.0;
    }
}

impl Sub for Amount {
    type Output = Amount;

    fn sub(self, other: Self) -> Self::Output {
        Amount(self.0 - other.0)
    }
}

#[derive(Hash, Eq, Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Client(u16);

impl Client {
    pub fn new(id: u16) -> Self {
        Client(id)
    }
}

#[derive(Hash, Eq, Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TransactionId(u32);

impl TransactionId {
    pub fn new(id: u32) -> Self {
        TransactionId(id)
    }
}

#[derive(Debug, Clone)]
pub struct Failure {
    pub client: Client,
    pub tx: TransactionId,
    pub reason: String,
}

impl Failure {
    pub fn new(client: Client, tx: TransactionId, reason: String) -> Self {
        Failure { client, tx, reason }
    }

    pub fn insufficient_funds(client: Client, tx: TransactionId) -> Self {
        Failure {
            client,
            tx,
            reason: "Insufficient funds".to_string(),
        }
    }

    pub fn no_wallet(client: Client, tx: TransactionId) -> Self {
        Failure {
            client,
            tx,
            reason: "No wallet found for client".to_string(),
        }
    }
}
