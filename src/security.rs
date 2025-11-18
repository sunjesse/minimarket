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
            sig_tx,
            sym: sym.into(),
        }
    }

    pub fn buy_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_nowait(OrderType::MarketBuy, to.order)
    }

    pub fn sell_wait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_wait(OrderType::LimitSell, to)
    }

    pub fn sell_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_nowait(OrderType::MarketSell, to.order)
    }

    pub fn buy_wait(&mut self, to: TimedOrder) -> Option<Order> {
        self.consume_wait(OrderType::LimitBuy, to)
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

    fn consume_wait(&mut self, kind: OrderType, mut to: TimedOrder) -> Option<Order> {
        // CORRECTNESS: assume kind is ony limitbuy or limitsell
        // we assume this is satisfied by the caller for now.

        let is_limit_buy: bool = OrderType::LimitBuy == kind;

        let mut order = to.order.clone();
        let mut i: usize = 0;
        let mut clients: Vec<Order> = Vec::new();

        let v: &mut Vec<TimedOrder> = if is_limit_buy {
            &mut self.ask
        } else {
            &mut self.bid
        };

        while order.quantity > 0 {
            if let Some(cur) = v.first_mut() {
                if (is_limit_buy && cur.order.price > order.price)
                    || (!is_limit_buy && cur.order.price < order.price)
                {
                    break;
                }
                // TODO: must avg out the price across the orders
                // to compute the avg price when signalling back limit order's client.
                let t: usize = cur.order.quantity.min(order.quantity);
                cur.order.quantity -= t;
                order.quantity -= t;

                if is_limit_buy {
                    self._q -= t;
                }

                clients.push(Order {
                    id: cur.order.id,
                    sym: cur.order.sym.clone(),
                    quantity: t,
                    price: cur.order.price,
                    kind: Some(kind),
                });

                if cur.order.quantity == 0 {
                    i += 1;
                } else {
                    break;
                }
            }
        }

        v.drain(..i);

        let _ = self.sig_tx.try_send(Arc::new(clients));
        if order.quantity > 0 {
            to.order.quantity = order.quantity;
            binary_insert_by_cmp(v, to, |a, b| {
                let cmp_price = if is_limit_buy {
                    b.order.price.cmp(&a.order.price)
                } else {
                    a.order.price.cmp(&b.order.price)
                };
                cmp_price.then(a.dt.cmp(&b.dt))
            });
            return None;
        }
        Some(order)
    }

    fn consume_nowait(&mut self, kind: OrderType, order: Order) -> Option<Order> {
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

        let mut clients: Vec<Order> = Vec::new();

        while c > 0 {
            let Some(cur) = v.get_mut(i) else { break };

            let t: usize = c.min(cur.order.quantity);

            x += cur.order.price * (t as i64);
            c -= t;
            cur.order.quantity -= t;

            clients.push(Order {
                id: cur.order.id,
                sym: order.sym.clone(),
                quantity: t,
                price: cur.order.price,
                kind: Some(kind),
            });

            if cur.order.quantity == 0 {
                i += 1
            } else {
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
        let _ = self.sig_tx.try_send(Arc::new(clients));

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
            broadcast_tx,
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
                let v: Option<i64> = kv.value().current_price();
                SecPrice::new(k, v)
            })
            .collect();
        SecPriceVec(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SecPrice {
    sym: Symbol,
    price: Option<i64>,
    dt: SystemTime,
}

impl SecPrice {
    pub fn new(sym: Symbol, price: Option<i64>) -> Self {
        Self {
            sym,
            price,
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
    pub fn new(v: Vec<(Symbol, Option<i64>)>) -> Self {
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

#[derive(Debug, Default)]
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

    // TODO: clearly there is some consolidating we can do to have less repetition among
    // the .mean(..) and .variance(..) methods.

    // TODO #2: Moreover, should we be returning Option<f64>? can expect to get
    // floating point numbers from these calculations.
    pub fn mean(&self, sym: Symbol, start: SystemTime, end: SystemTime) -> Option<i64> {
        if start > end {
            eprintln!("start time cannot be after end");
            return None;
        }
        // TODO: optimize
        if let Some(v) = self._t.get(&sym)
            && let Ok(s) = v.binary_search_by(|x| x.dt.cmp(&start))
            && let Ok(e) = v.binary_search_by(|x| x.dt.cmp(&end))
        {
            let r: i64 = v[s..=e]
                .iter()
                .map(|x| x.price.unwrap_or_default())
                .sum::<i64>()
                / ((e - s + 1) as i64);
            return Some(r);
        }

        None
    }

    pub fn variance(&self, sym: Symbol, start: SystemTime, end: SystemTime) -> Option<i64> {
        let mu = self.mean(sym.clone(), start, end)?;

        if let Some(v) = self._t.get(&sym)
            && let Ok(s) = v.binary_search_by(|x| x.dt.cmp(&start))
            && let Ok(e) = v.binary_search_by(|x| x.dt.cmp(&end))
        {
            let r: i64 = v[s..=e]
                .iter()
                .map(|x| if let Some(p) = x.price { p * p } else { 0_i64 })
                .sum::<i64>()
                / ((e - s + 1) as i64);

            let var: i64 = r - mu * mu;
            return Some(var);
        }

        None
    }

    pub fn std_deviation(&self, sym: Symbol, start: SystemTime, end: SystemTime) -> Option<i64> {
        let var = self.variance(sym.clone(), start, end)?;
        Some(var.isqrt()) // TODO: currently returns value rounded down as it's all i64
    }
}
