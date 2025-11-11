use dashmap::DashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::connection::Hub;
use crate::order::{Order, OrderType};
use crate::utils::binary_insert_by_key;

#[derive(Debug)]
pub struct Book {
    id: usize,
    _q: usize,       // quantity
    bid: Vec<Order>, // sorted desc
    ask: Vec<Order>, // sorted asc
    hub: Arc<Hub>,
}

impl Book {
    // TODO: none of this is thread-safe,
    // wrap all in mutex?
    pub fn new(id: usize, quantity: usize, hub: Arc<Hub>) -> Self {
        Self {
            id: id,
            _q: quantity,
            bid: Vec::new(),
            ask: Vec::new(),
            hub: hub,
        }
    }

    pub fn buy_nowait(&mut self, req_sz: usize) -> Option<Order> {
        self.consume_nowait(OrderType::MarketBuy, req_sz)
    }

    pub fn sell_wait(&mut self, order: Order) {
        binary_insert_by_key(&mut self.ask, order, |o| o.price, false);
        self._q += order.quantity;
    }

    pub fn sell_nowait(&mut self, req_sz: usize) -> Option<Order> {
        self.consume_nowait(OrderType::MarketSell, req_sz)
    }

    pub fn buy_wait(&mut self, order: Order) {
        // TODO: double check this logic, doesn't feel right lol
        binary_insert_by_key(&mut self.ask, order, |o| o.price, true);
        //self._q -= order.quantity;
    }

    pub fn spread(self) -> Option<(f32, f32)> {
        if self.ask.len() == 0 || self.bid.len() == 0 {
            return None;
        }
        Some((self.bid[0].price, self.ask[0].price))
    }

    fn consume_nowait(&mut self, kind: OrderType, req_sz: usize) -> Option<Order> {
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

        let mut _clients: Vec<Order> = Vec::new();

        while c > 0 && i < v.len() {
            if v[i].quantity <= c {
                c -= v[i].quantity;
                x += v[i].price * (v[i].quantity as f32);
                v[i].quantity = 0;
                i += 1;
                _clients.push(Order {
                    id: v[i].id,
                    quantity: v[i].quantity,
                    price: v[i].price,
                    kind: None,
                });
            } else {
                x += v[i].price * (c as f32);
                v[i].quantity -= c;
                c = 0;
                _clients.push(Order {
                    id: v[i].id,
                    quantity: c,
                    price: v[i].price,
                    kind: None,
                });
            }
        }

        println!("c {:?} req {:?}", c, req_sz);
        if c > 0 {
            return None;
        }

        // otherwise order was successful
        v.drain(..i);
        println!("drained {:?}", v);
        if kind == OrderType::MarketBuy {
            self._q -= req_sz;
        } else if kind == OrderType::MarketSell {
            self._q += req_sz;
        }

        // signal to clients; TODO: we should offload this to a separate thread.
        self.hub.broadcast_to(_clients);

        Some(Order {
            id: Uuid::new_v4(), // TODO: filler right now, get actual connection id
            quantity: req_sz,
            price: x / (req_sz as f32),
            kind: Some(kind),
        })
    }
}

pub struct Exchange {
    books: Arc<DashMap<String, Book>>,
}

impl Exchange {
    pub fn new() -> Self {
        Self {
            books: Arc::new(DashMap::new()),
        }
    }
}
