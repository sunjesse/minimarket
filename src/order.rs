use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// TODO: move this struct to interning in the future
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(pub Arc<str>);

impl Symbol {
    pub fn new<S: AsRef<str>>(s: S) -> Self {
        Self(Arc::<str>::from(s.as_ref().trim().to_uppercase()))
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Symbol(Arc::<str>::from(s.trim().to_uppercase()))
    }
}
impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Symbol(Arc::<str>::from(s.trim().to_uppercase()))
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderType {
    MarketSell,
    LimitSell,
    MarketBuy,
    LimitBuy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid, // client id
    pub sym: Symbol,
    pub quantity: usize,
    pub price: f32,
    pub kind: Option<OrderType>,
}

impl From<&Bytes> for Order {
    fn from(b: &Bytes) -> Self {
        bincode::deserialize::<Order>(b).expect("error deserializing")
    }
}

impl From<&Order> for Bytes {
    fn from(o: &Order) -> Self {
        let v = bincode::serialize(o).expect("error serializing");
        Bytes::from(v)
    }
}
