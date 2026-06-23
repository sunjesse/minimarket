use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::order::{Order, OrderType, Symbol, TimedOrder};
use crate::utils::binary_insert_by_cmp;

#[derive(Debug, Serialize)]
pub struct Security {
    _id: Uuid,
    // TODO: move to using BTreeMaps keyed by price levels
    // and a vecdeque per entry. Currently, insertions are O(n) as well!!
    bid: BTreeMap<i64, VecDeque<TimedOrder>>, // sorted desc
    ask: BTreeMap<i64, VecDeque<TimedOrder>>, // sorted asc
    bid_q: usize,
    ask_q: usize,
    #[serde(skip)]
    sig_tx: mpsc::Sender<Arc<Vec<Order>>>,
    #[allow(unused)]
    sym: Symbol,
}

impl Security {
    // NOTE: Not thread safe - but this is fine
    // as we pin each security to exactly one thread.
    // Hence, we don't need to wrap the internal fields
    // in Arc<Mutex<T>>.
    pub fn new<S: Into<Symbol>>(sym: S, sig_tx: mpsc::Sender<Arc<Vec<Order>>>) -> Self {
        Self {
            _id: Uuid::new_v4(),
            bid: BTreeMap::new(),
            ask: BTreeMap::new(),
            bid_q: 0,
            ask_q: 0,
            sig_tx,
            sym: sym.into(),
        }
    }

    pub fn buy_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.match_order(OrderType::MarketBuy, to)
    }

    pub fn sell_wait(&mut self, to: TimedOrder) -> Option<Order> {
        self.match_order(OrderType::LimitSell, to)
    }

    pub fn sell_nowait(&mut self, to: TimedOrder) -> Option<Order> {
        self.match_order(OrderType::MarketSell, to)
    }

    pub fn buy_wait(&mut self, to: TimedOrder) -> Option<Order> {
        self.match_order(OrderType::LimitBuy, to)
    }

    pub fn spread(&self) -> Option<(i64, i64)> {
        let (&bid, _) = self.bid.last_key_value()?;
        let (&ask, _) = self.ask.first_key_value()?;
        Some((bid, ask))
    }

    pub fn current_price(&self) -> Option<i64> {
        let (lb, ub) = self.spread()?;
        Some((ub + lb) / 2)
    }

    fn get_q(&self, kind: &OrderType) -> usize {
        match kind {
            OrderType::MarketBuy => self.ask_q,
            OrderType::MarketSell => self.bid_q,
            _ => unreachable!(),
        }
    }

    fn match_order(&mut self, kind: OrderType, mut to: TimedOrder) -> Option<Order> {
        let is_limit_order: bool =
            kind == OrderType::LimitBuy || kind == OrderType::LimitSell;
        let is_buy: bool =
            kind == OrderType::LimitBuy || kind == OrderType::MarketBuy;

        if !is_limit_order && to.order.quantity > self.get_q(&kind) {
            return None;
        }

        let v: &mut BTreeMap<i64, VecDeque<TimedOrder>> = match kind {
            OrderType::MarketBuy => &mut self.ask,
            OrderType::LimitBuy => &mut self.ask,
            OrderType::MarketSell => &mut self.bid,
            OrderType::LimitSell => &mut self.bid,
        };

        let requested_quantity: usize = to.order.quantity;
        let mut total_cost: i64 = 0;
        let mut clients: Vec<Order> = Vec::new();

        loop {
            if to.order.quantity == 0 {
                break;
            }

            let entry = if is_buy {
                v.first_entry()
            } else {
                v.last_entry()
            };

            let Some(mut level) = entry else { break };

            let price_level: i64 = *level.key();

            if is_limit_order {
                if (is_buy && price_level > to.order.price)
                    || (!is_buy && price_level < to.order.price)
                {
                    break;
                }
            }

            let dq = level.get_mut();

            while to.order.quantity > 0 {
                let Some(cur) = dq.front_mut() else { break };
                let t: usize = cur.order.quantity.min(to.order.quantity);
                if t == 0 {
                    break;
                }
                total_cost += (t as i64) * cur.order.price;
                cur.order.quantity -= t;
                to.order.quantity -= t;
                if is_buy {
                    self.ask_q -= t;
                } else {
                    self.bid_q -= t;
                }

                clients.push(Order {
                    id: cur.order.id,
                    sym: to.order.sym.clone(),
                    quantity: t,
                    price: cur.order.price,
                    kind: Some(kind),
                });

                if cur.order.quantity == 0 {
                    dq.pop_front();
                } else {
                    break;
                }
            }
            if dq.is_empty() {
                level.remove();
            }
        }

        let _ = self.sig_tx.try_send(Arc::new(clients));

        if to.order.quantity > 0 {
            if !is_limit_order {
                return None;
            } else {
                if is_buy {
                    self.ask_q += to.order.quantity
                } else {
                    self.bid_q += to.order.quantity
                };
                v.entry(to.order.price).or_default().push_back(to);
                return None;
            }
        }

        let filled_quantity: usize = requested_quantity - to.order.quantity;
        to.order.price = total_cost / filled_quantity as i64;
        to.order.quantity = filled_quantity;
        Some(to.order)
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
