use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::SystemTime};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::order::{Order, OrderType, Symbol, TimedOrder};
use crate::utils::binary_insert_by_cmp;

#[derive(Debug)]
pub struct Security {
    _id: Uuid,
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

    fn get_q(&self) -> usize {
        self.ask.iter().map(|o| o.order.quantity).sum::<usize>()
    }

    fn consume_wait(&mut self, kind: OrderType, mut to: TimedOrder) -> Option<Order> {
        // CORRECTNESS: assume kind is ony limitbuy or limitsell
        // we assume this is satisfied by the caller for now.

        let is_limit_buy: bool = OrderType::LimitBuy == kind;

        let mut order: Order = to.order.clone();
        let requested: usize = order.quantity;

        let mut total_cost: i64 = 0;
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
                if t == 0 {
                    break;
                }
                total_cost += (t as i64) * cur.order.price;
                cur.order.quantity -= t;
                order.quantity -= t;

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
            } else {
                break;
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

        order.price = total_cost / (requested - order.quantity) as i64;
        Some(order)
    }

    fn consume_nowait(&mut self, kind: OrderType, order: Order) -> Option<Order> {
        let requested: usize = order.quantity;
        if requested > self.get_q() {
            return None;
        }

        let v: &mut Vec<TimedOrder> = if kind == OrderType::MarketBuy {
            &mut self.ask
        } else if kind == OrderType::MarketSell {
            &mut self.bid
        } else {
            unreachable!();
        };

        let mut left: usize = requested;
        let mut x: i64 = 0_i64;
        let mut i: usize = 0;

        let mut clients: Vec<Order> = Vec::new();

        while left > 0 {
            let Some(cur) = v.get_mut(i) else { break };

            let t: usize = left.min(cur.order.quantity);

            if t == 0 {
                break;
            }
            x += cur.order.price * (t as i64);
            left -= t;
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

        if left > 0 {
            return None;
        }

        // otherwise order was successful
        v.drain(..i);

        // signal to clients;
        let _ = self.sig_tx.try_send(Arc::new(clients));

        Some(Order {
            id: order.id,
            sym: order.sym.clone(),
            quantity: requested,
            price: x / (requested as i64),
            kind: Some(kind),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecPrice {
    pub sym: Symbol,
    pub price: Option<i64>,
    pub dt: SystemTime,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecPriceVec {
    pub prices: Vec<SecPrice>,
}

impl SecPriceVec {
    pub fn new(v: Vec<(Symbol, Option<i64>)>) -> Self {
        // TODO: assuming v is sorted chronologically increasing already.
        let prices: Vec<SecPrice> =
            v.into_iter().map(|(s, p)| SecPrice::new(s, p)).collect();

        Self { prices }
    }

    // TODO: These methods should be used for symbol -> time-series.
    // Current, SecPriceVec only holds a current price per symbol.
    pub fn add_price(&mut self, sp: SecPrice) {
        binary_insert_by_cmp(&mut self.prices, sp, |a, b| a.dt.cmp(&b.dt));
    }

    pub fn get_most_recent_price(&self) -> Option<i64> {
        if let Some(last) = self.prices.last() {
            return last.price;
        }
        None
    }

    // TODO #1: Moreover, should we be returning Option<f64>? can expect to get
    // floating point numbers from these calculations.
    pub fn mean(&self, start: SystemTime, end: SystemTime) -> Option<i64> {
        if start > end {
            eprintln!("start time cannot be after end");
            return None;
        }
        if let Ok(s) = self.prices.binary_search_by(|x| x.dt.cmp(&start))
            && let Ok(e) = self.prices.binary_search_by(|x| x.dt.cmp(&end))
        {
            let r: i64 = self.prices[s..=e]
                .iter()
                .map(|x| x.price.unwrap_or_default())
                .sum::<i64>()
                / ((e - s + 1) as i64);
            return Some(r);
        }

        None
    }

    pub fn variance(&self, start: SystemTime, end: SystemTime) -> Option<i64> {
        let mu = self.mean(start, end)?;

        if let Ok(s) = self.prices.binary_search_by(|x| x.dt.cmp(&start))
            && let Ok(e) = self.prices.binary_search_by(|x| x.dt.cmp(&end))
        {
            let r: i64 = self.prices[s..=e]
                .iter()
                .map(|x| if let Some(p) = x.price { p * p } else { 0_i64 })
                .sum::<i64>()
                / ((e - s + 1) as i64);

            let var: i64 = r - mu * mu;
            return Some(var);
        }

        None
    }

    pub fn std_deviation(&self, start: SystemTime, end: SystemTime) -> Option<i64> {
        let var = self.variance(start, end)?;
        Some(var.isqrt()) // TODO: currently returns value rounded down as it's all i64
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
