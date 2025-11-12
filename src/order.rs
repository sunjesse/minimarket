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

pub fn order_to_bytes(order: &Order) -> Bytes {
    let v = bincode::serialize(order).expect("serialize");
    Bytes::from(v)
}

pub fn bytes_to_order(bytes: &Bytes) -> Order {
    bincode::deserialize::<Order>(bytes).expect("deserialize")
}
