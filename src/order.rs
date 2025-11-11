use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum OrderType {
    MarketSell,
    LimitSell,
    MarketBuy,
    LimitBuy,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
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
