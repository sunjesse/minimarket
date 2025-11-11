use std::sync::Arc;
use uuid::Uuid;

use crate::connection::Hub;
use crate::order::{Order, OrderType};
use crate::utils::binary_insert_by_key;

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
    fn new(id: usize, quantity: usize, hub: Arc<Hub>) -> Self {
        Book {
            id: id,
            _q: quantity,
            bid: Vec::new(),
            ask: Vec::new(),
            hub: hub,
        }
    }

    fn buy_nowait(&mut self, req_sz: usize) -> Option<Order> {
        if req_sz >= self._q {
            return None;
        }

        let mut c: usize = 0;
        let mut x: f32 = 0_f32;
        let mut i: usize = 0;

        let mut _clients: Vec<Order> = Vec::new();

        for ord in self.ask.iter_mut() {
            // TODO: cleanup this implementation, not the cleanest
            // Also need to signal to sellers that their stock was bought
            let t: usize = ord.quantity + c;
            i += 1;
            if t >= req_sz {
                let diff: usize = t - req_sz;
                x += ord.price * (diff as f32);
                c += diff;
                ord.quantity -= diff;
                _clients.push(Order {
                    id: ord.id,
                    quantity: diff,
                    price: ord.price,
                    kind: None,
                });
                break;
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

        if c < req_sz {
            return None;
        }

        // otherwise order was successful
        self.ask.drain(i..);
        self._q -= req_sz;
        // signal to clients; we should offload this to a separate thread.
        self.hub.broadcast_to(_clients);

        Some(Order {
            id: Uuid::new_v4(), // TODO: filler right now, get actual connection id
            quantity: c,
            price: x / (c as f32),
            kind: Some(OrderType::MarketBuy),
        })
    }

    fn sell_wait(&mut self, order: Order) {
        binary_insert_by_key(&mut self.ask, order, |o| o.price);
        self._q += order.quantity;
    }
}
