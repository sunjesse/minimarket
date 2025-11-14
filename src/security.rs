use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::order::{Order, OrderType, Symbol};
use crate::utils::binary_insert_by_key;

#[derive(Debug)]
pub struct Security {
    _id: Uuid,
    _q: usize,       // materalized quantity of pending sell orders
    bid: Vec<Order>, // sorted desc
    ask: Vec<Order>, // sorted asc
    sig_tx: mpsc::Sender<Arc<Vec<Order>>>,
    sym: Symbol,
}

impl Security {
    // NOT THREAD SAFE! Wrap in Arc<Mutex<T>>.
    pub fn new<S: Into<Symbol>>(sym: S, sig_tx: mpsc::Sender<Arc<Vec<Order>>>) -> Self {
        Self {
            _id: Uuid::new_v4(),
            _q: 0,
            bid: Vec::new(),
            ask: Vec::new(),
            sig_tx: sig_tx,
            sym: sym.into(),
        }
    }

    pub fn buy_nowait(&mut self, order: Order) -> Option<Order> {
        self.consume_nowait(OrderType::MarketBuy, order)
    }

    pub fn sell_wait(&mut self, mut order: Order) -> Option<Order> {
        // first check if there is a bid that matches
        let mut i: usize = 0;
        while order.quantity > 0 {
            if let Some(bb) = self.bid.first_mut() {
                if bb.price < order.price {
                    break;
                }

                let t: usize = bb.quantity.min(order.quantity); // the traded amount
                bb.quantity -= t;
                order.quantity -= t;
                self._q -= t;

                if bb.quantity == 0 {
                    i += 1;
                }
            } else {
                break;
            }
        }
        self.bid.drain(..i);
        // otherwise we insert it into ask
        if order.quantity > 0 {
            binary_insert_by_key(&mut self.ask, order.clone(), |o| o.price, false);
            self._q += order.quantity;
            return None;
        }
        Some(order)
    }

    pub fn sell_nowait(&mut self, order: Order) -> Option<Order> {
        self.consume_nowait(OrderType::MarketSell, order)
    }

    pub fn buy_wait(&mut self, mut order: Order) -> Option<Order> {
        let mut i: usize = 0;
        while order.quantity > 0 {
            if let Some(ba) = self.ask.first_mut() {
                if ba.price > order.price {
                    break;
                }
                let t: usize = ba.quantity.min(order.quantity);
                ba.quantity -= t;
                order.quantity -= t;
                self._q -= t;

                if ba.quantity == 0 {
                    i += 1;
                }
            } else {
                break;
            }
        }
        self.ask.drain(..i);
        if order.quantity > 0 {
            binary_insert_by_key(&mut self.bid, order, |o| o.price, true);
            return None;
        }
        Some(order)
    }

    pub fn spread(&self) -> Option<(f32, f32)> {
        if self.ask.is_empty() || self.bid.is_empty() {
            return None;
        }
        Some((self.bid[0].price, self.ask[0].price))
    }

    pub fn current_price(&self) -> Option<f32> {
        if let Some((lb, ub)) = self.spread() {
            return Some((ub + lb) / 2_f32);
        }
        None
    }

    fn consume_nowait(&mut self, kind: OrderType, order: Order) -> Option<Order> {
        // TODO: clean up all this spaghetti
        let req_sz: usize = order.quantity;
        if req_sz >= self._q {
            return None;
        }

        let v: &mut Vec<Order> = if kind == OrderType::MarketBuy {
            &mut self.ask
        } else if kind == OrderType::MarketSell {
            &mut self.bid
        } else {
            unreachable!();
        };

        let mut c: usize = req_sz;
        let mut x: f32 = 0_f32;
        let mut i: usize = 0;

        let mut _clients = Vec::new();

        while c > 0 && i < v.len() {
            if v[i].quantity <= c {
                c -= v[i].quantity;
                x += v[i].price * (v[i].quantity as f32);
                i += 1;
                _clients.push(Order {
                    id: v[i].id,
                    sym: order.sym.clone(),
                    quantity: v[i].quantity,
                    price: v[i].price,
                    kind: Some(kind),
                });
                v[i].quantity = 0;
            } else {
                x += v[i].price * (c as f32);
                v[i].quantity -= c;
                c = 0;
                _clients.push(Order {
                    id: v[i].id,
                    sym: order.sym.clone(),
                    quantity: c,
                    price: v[i].price,
                    kind: Some(kind),
                });
            }
        }

        if c > 0 {
            return None;
        }

        // otherwise order was successful
        v.drain(..i);
        if kind == OrderType::MarketBuy {
            self._q -= req_sz;
        } else if kind == OrderType::MarketSell {
            self._q += req_sz;
        }

        // signal to clients;
        let _ = self.sig_tx.try_send(Arc::new(_clients));

        Some(Order {
            id: order.id,
            sym: order.sym.clone(),
            quantity: req_sz,
            price: x / (req_sz as f32),
            kind: Some(kind),
        })
    }
}

pub struct Exchange {
    securities: Arc<DashMap<Symbol, Security>>,
    broadcast_tx: mpsc::Sender<Arc<Vec<Order>>>,
}

impl Exchange {
    pub fn new(broadcast_tx: mpsc::Sender<Arc<Vec<Order>>>) -> Self {
        Self {
            securities: Arc::new(DashMap::new()),
            broadcast_tx: broadcast_tx,
        }
    }

    pub fn add_order(&self, order: Order) -> Option<Order> {
        let mut entry = self
            .securities
            .entry(order.sym.clone())
            .or_insert(Security::new(order.sym.clone(), self.broadcast_tx.clone()));

        let sec = entry.value_mut();

        match order.kind {
            Some(OrderType::MarketBuy) => sec.buy_nowait(order),
            Some(OrderType::MarketSell) => sec.sell_nowait(order),
            Some(OrderType::LimitSell) => sec.sell_wait(order),
            Some(OrderType::LimitBuy) => sec.buy_wait(order),
            None => None,
        }
    }

    pub fn list_all_security_prices(&self) -> SecPriceVec {
        let v: Vec<SecPrice> = self
            .securities
            .iter()
            .map(|kv| {
                let k: Symbol = kv.key().clone();
                let v: f32 = kv.value().current_price().unwrap_or(-1.0);
                SecPrice((k, v))
            })
            .collect();
        SecPriceVec(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SecPrice(pub (Symbol, f32));

impl SecPrice {
    pub fn new(sym: Symbol, price: f32) -> Self {
        Self((sym, price))
    }
}

impl From<&Bytes> for SecPrice {
    fn from(b: &Bytes) -> Self {
        bincode::deserialize(b).expect("deserialize")
    }
}

impl From<&SecPrice> for Bytes {
    fn from(sp: &SecPrice) -> Self {
        Bytes::from(bincode::serialize(sp).expect("serialize"))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SecPriceVec(pub Vec<SecPrice>);

impl SecPriceVec {
    pub fn new(v: Vec<(Symbol, f32)>) -> Self {
        Self(v.into_iter().map(|(s, p)| SecPrice((s, p))).collect())
    }
}

impl From<&Bytes> for SecPriceVec {
    fn from(b: &Bytes) -> Self {
        bincode::deserialize(b).expect("deserialize")
    }
}

impl From<&SecPriceVec> for Bytes {
    fn from(v: &SecPriceVec) -> Self {
        Bytes::from(bincode::serialize(v).expect("serialize"))
    }
}
