use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderType {
    MarketSell,
    LimitSell,
    MarketBuy,
    LimitBuy,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid, // client id
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
