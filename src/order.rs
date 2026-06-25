use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{fmt, time::SystemTime};
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

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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
    pub client_id: Option<Uuid>, // client id
    pub sym: Symbol,
    pub quantity: usize,
    pub price: i64, // in cents
    pub kind: Option<OrderType>,
    order_id: u16,
    cancelled: bool,
}

impl Order {
    pub fn new(
        client_id: Option<Uuid>,
        sym: Symbol,
        quantity: usize,
        price: i64,
        kind: Option<OrderType>,
    ) -> Self {
        Self {
            client_id: client_id,
            sym: sym,
            quantity: quantity,
            price: price,
            kind: kind,
            order_id: 0, // 0 is unavailable
            cancelled: false,
        }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn get_order_id(&self) -> u16 {
        self.order_id
    }

    pub fn set_client_id(mut self, client_id: Uuid) -> Self {
        self.client_id = Some(client_id);
        self
    }

    pub fn set_order_id(mut self, order_id: u16) -> Self {
        self.order_id = order_id;
        self
    }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimedOrder {
    pub order: Order,
    pub dt: SystemTime,
}

impl TimedOrder {
    pub fn new(order: Order) -> Self {
        Self {
            order,
            dt: SystemTime::now(),
        }
    }
}
