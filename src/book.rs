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

        let mut c: usize = 0;
        let mut x: f32 = 0_f32;
        let mut i: usize = 0;

        let mut _clients: Vec<Order> = Vec::new();

        for ord in v.iter_mut() {
            // TODO: Fix this spaghetti
            if c == req_sz {
                break;
            }
            let t: usize = ord.quantity + c;
            i += 1;
            if t >= req_sz {
                let diff: usize = req_sz - c;
                x += ord.price * (diff as f32);
                c += diff;
                ord.quantity -= diff;
                _clients.push(Order {
                    id: ord.id,
                    quantity: diff,
                    price: ord.price,
                    kind: None,
                });
            } else {
                x += ord.price * (ord.quantity as f32);
                c += ord.quantity;
                _clients.push(Order {
                    id: ord.id,
                    quantity: ord.quantity,
                    price: ord.price,
                    kind: None,
                });
            }
        }

        println!("c {:?} req {:?}", c, req_sz);
        if c < req_sz {
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
            quantity: c,
            price: x / (c as f32),
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
