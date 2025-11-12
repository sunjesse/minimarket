use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::order::{Order, OrderType, Symbol};
use crate::utils::binary_insert_by_key;

#[derive(Debug)]
pub struct Security {
    _id: Uuid,
    _q: usize,       // quantity
    bid: Vec<Order>, // sorted desc
    ask: Vec<Order>, // sorted asc
    sig_tx: mpsc::Sender<Arc<Vec<Order>>>,
    sym: Symbol,
}

impl Security {
    // NOT THREAD SAFE! Wrap in Arc<Mutex<T>>.
    pub fn new<S: AsRef<str>>(sym: S, sig_tx: mpsc::Sender<Arc<Vec<Order>>>) -> Self {
        Self {
            _id: Uuid::new_v4(),
            _q: 0,
            bid: Vec::new(),
            ask: Vec::new(),
            sig_tx: sig_tx,
            sym: Symbol::new(sym),
        }
    }

    pub fn buy_nowait(&mut self, order: Order) -> Option<Order> {
        self.consume_nowait(OrderType::MarketBuy, order)
    }

    pub fn sell_wait(&mut self, order: Order) -> Option<Order> {
        // TODO: remove clone, pass in refs
        binary_insert_by_key(&mut self.ask, order.clone(), |o| o.price, false);
        self._q += order.quantity;
        None
    }

    pub fn sell_nowait(&mut self, order: Order) -> Option<Order> {
        self.consume_nowait(OrderType::MarketSell, order)
    }

    pub fn buy_wait(&mut self, order: Order) -> Option<Order> {
        binary_insert_by_key(&mut self.bid, order, |o| o.price, true);
        None
    }

    pub fn spread(self) -> Option<(f32, f32)> {
        if self.ask.len() == 0 || self.bid.len() == 0 {
            return None;
        }
        Some((self.bid[0].price, self.ask[0].price))
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
            panic!("unexpected order type");
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

#[allow(unused)]
pub struct Exchange {
    securities: Arc<DashMap<String, Security>>,
}

#[allow(unused)]
impl Exchange {
    pub fn new() -> Self {
        Self {
            securities: Arc::new(DashMap::new()),
        }
    }
}
