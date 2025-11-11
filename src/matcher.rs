use crate::utils::binary_insert_by_key;

#[derive(Copy, Clone)]
pub struct Order {
    // TODO: keep track of which client ordered it, so we can
    // send notifications back
    quantity: usize,
    price: f32,
}

pub struct Book {
    id: usize,
    q: usize,        // quantity
    bid: Vec<Order>, // sorted desc
    ask: Vec<Order>, // sorted asc
}

impl Book {
    // TODO: none of this is thread-safe,
    // wrap all in mutex?
    fn new(id: usize, quantity: usize) -> Self {
        Book {
            id: id,
            q: quantity,
            bid: Vec::new(),
            ask: Vec::new(),
        }
    }

    fn buy_nowait(&mut self, req_sz: usize) -> Option<Order> {
        if req_sz >= self.q {
            return None;
        }

        let mut c: usize = 0;
        let mut x: f32 = 0_f32;
        let mut i: usize = 0;

        for ord in self.ask.iter_mut() {
            // TODO: cleanup this implementation, not the cleanest
            let t: usize = ord.quantity + c;
            i += 1;
            if t >= req_sz {
                let diff: usize = t - req_sz;
                x += ord.price * (diff as f32);
                c += diff;
                ord.quantity -= diff;
                break;
            } else {
                x += ord.price * (ord.quantity as f32);
                c += ord.quantity;
            }
        }

        if c < req_sz {
            return None;
        }

        // otherwise order was successful
        self.ask.drain(i..);
        self.q -= req_sz;
        Some(Order {
            quantity: c,
            price: x / (c as f32),
        })
    }

    fn sell_wait(&mut self, order: Order) {
        binary_insert_by_key(&mut self.ask, order, |o| o.price);
    }
}
