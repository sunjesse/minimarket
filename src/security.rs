use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::SystemTime};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::order::{Order, OrderType, Symbol, TimedOrder};
use crate::utils::binary_insert_by_cmp;

#[derive(Debug)]
pub struct Security {
    _id: Uuid,
    _q: usize,            // materalized quantity of pending sell orders
    bid: Vec<TimedOrder>, // sorted desc
    ask: Vec<TimedOrder>, // sorted asc
    sig_tx: mpsc::Sender<Arc<Vec<Order>>>,
    #[allow(unused)]
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

    pub fn buy_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_nowait(OrderType::MarketBuy, to.order)
    }

    pub fn sell_wait(&mut self, mut to: TimedOrder) -> Option<Order> {
        // first check if there is a bid that matches
        let mut order = to.order.clone(); // TODO: tmp (1)
        let mut i: usize = 0;
        while order.quantity > 0 {
            if let Some(bb) = self.bid.first_mut() {
                if bb.order.price < order.price {
                    break;
                }

                let t: usize = bb.order.quantity.min(order.quantity); // the traded amount
                bb.order.quantity -= t;
                order.quantity -= t;
                self._q -= t;

                if bb.order.quantity == 0 {
                    i += 1;
                }
            } else {
                break;
            }
        }
        self.bid.drain(..i);
        // otherwise we insert it into ask
        if order.quantity > 0 {
            to.order.quantity = order.quantity; // TODO: tmp (1)
            binary_insert_by_cmp(&mut self.ask, to, |a, b| {
                a.order
                    .price
                    .cmp(&b.order.price) // asc
                    .then(a.dt.cmp(&b.dt)) // asc
            });
            self._q += order.quantity;
            return None;
        }
        Some(order)
    }

    pub fn sell_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_nowait(OrderType::MarketSell, to.order)
    }

    pub fn buy_wait(&mut self, mut to: TimedOrder) -> Option<Order> {
        let mut order = to.order.clone();
        let mut i: usize = 0;
        while order.quantity > 0 {
            if let Some(ba) = self.ask.first_mut() {
                if ba.order.price > order.price {
                    break;
                }
                let t: usize = ba.order.quantity.min(order.quantity);
                ba.order.quantity -= t;
                order.quantity -= t;
                self._q -= t;

                if ba.order.quantity == 0 {
                    i += 1;
                }
            } else {
                break;
            }
        }
        self.ask.drain(..i);
        if order.quantity > 0 {
            to.order.quantity = order.quantity;
            binary_insert_by_cmp(&mut self.bid, to, |a, b| {
                b.order
                    .price
                    .cmp(&a.order.price) // desc
                    .then(a.dt.cmp(&b.dt)) // asc
            });
            return None;
        }
        Some(order)
    }

    pub fn spread(&self) -> Option<(i64, i64)> {
        if self.ask.is_empty() || self.bid.is_empty() {
            return None;
        }
        Some((self.bid[0].order.price, self.ask[0].order.price))
    }

    pub fn current_price(&self) -> Option<i64> {
        if let Some((lb, ub)) = self.spread() {
            return Some((ub + lb) / 2_i64);
        }
        None
    }

    fn consume_nowait(&mut self, kind: OrderType, order: Order) -> Option<Order> {
        // TODO: clean up all this spaghetti
        let req_sz: usize = order.quantity;
        if req_sz >= self._q {
            return None;
        }

        let v: &mut Vec<TimedOrder> = if kind == OrderType::MarketBuy {
            &mut self.ask
        } else if kind == OrderType::MarketSell {
            &mut self.bid
        } else {
            unreachable!();
        };

        let mut c: usize = req_sz;
        let mut x: i64 = 0_i64;
        let mut i: usize = 0;

        let mut _clients = Vec::new();

        while c > 0 && i < v.len() {
            let mut ord_at_i = Order {
                id: v[i].order.id,
                sym: order.sym.clone(),
                quantity: 0,
                price: v[i].order.price,
                kind: Some(kind),
            };
            if v[i].order.quantity <= c {
                c -= v[i].order.quantity;
                x += v[i].order.price * (v[i].order.quantity as i64);
                ord_at_i.quantity = v[i].order.quantity;
                _clients.push(ord_at_i);
                v[i].order.quantity = 0; // in theory not needed
                i += 1;
            } else {
                x += v[i].order.price * (c as i64);
                v[i].order.quantity -= c;
                ord_at_i.quantity = c;
                _clients.push(ord_at_i);
                break;
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
            price: x / (req_sz as i64),
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

    pub fn add_order(&self, to: TimedOrder) -> Option<Order> {
        let mut entry = self
            .securities
            .entry(to.order.sym.clone())
            .or_insert(Security::new(
                to.order.sym.clone(),
                self.broadcast_tx.clone(),
            ));

        let sec = entry.value_mut();

        match to.order.kind {
            Some(OrderType::MarketBuy) => sec.buy_nowait(to),
            Some(OrderType::MarketSell) => sec.sell_nowait(to),
            Some(OrderType::LimitSell) => sec.sell_wait(to),
            Some(OrderType::LimitBuy) => sec.buy_wait(to),
            None => None,
        }
    }

    pub fn list_all_security_prices(&self) -> SecPriceVec {
        let v: Vec<SecPrice> = self
            .securities
            .iter()
            .map(|kv| {
                let k: Symbol = kv.key().clone();
                let v: i64 = kv.value().current_price().unwrap_or(-1_i64);
                SecPrice::new(k, v)
            })
            .collect();
        SecPriceVec(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SecPrice {
    sym: Symbol,
    price: i64,
    dt: SystemTime,
}

impl SecPrice {
    pub fn new(sym: Symbol, price: i64) -> Self {
        Self {
            sym: sym,
            price: price,
            // TODO: in theory, the time should be passed in from Security.current_price()
            dt: SystemTime::now(),
        }
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
    pub fn new(v: Vec<(Symbol, i64)>) -> Self {
        Self(v.into_iter().map(|(s, p)| SecPrice::new(s, p)).collect())
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

#[derive(Debug)]
pub struct SecPriceSeries {
    _t: DashMap<Symbol, Vec<SecPrice>>,
}

impl SecPriceSeries {
    pub fn new() -> Self {
        Self { _t: DashMap::new() }
    }

    pub fn add_price(&mut self, sp: SecPrice) {
        if let Some(mut v) = self._t.get_mut(&sp.sym) {
            binary_insert_by_cmp(&mut v, sp, |a, b| a.dt.cmp(&b.dt));
        }
    }

    pub fn get_average_price(
        &self,
        sym: Symbol,
        start: SystemTime,
        end: SystemTime,
    ) -> Option<i64> {
        if let Some(v) = self._t.get(&sym) {
            // TODO: optimize
            if let Ok(s) = v.binary_search_by(|x| x.dt.cmp(&start))
                && let Ok(e) = v.binary_search_by(|x| x.dt.cmp(&end))
            {
                let r: i64 = v[s..=e].iter().map(|x| x.price).sum::<i64>() / ((e - s + 1) as i64);
                return Some(r);
            } else {
                return None;
            }
        }
        None
    }
}
