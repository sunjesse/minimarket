use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{StreamExt, pin_mut};
use rand::prelude::*;
use rand_distr::Normal;
use std::{
    env,
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

use minimarket::{
    ClientCommands, Frame, MAX_ACTIVE_ORDERS, Order, OrderCancel, OrderReceived,
    OrderType, Symbol,
};

type PriceTime = (i64, SystemTime);

fn parse_order(
    input: &str,
    order_index: Arc<Mutex<OrderIndex>>,
) -> Option<ClientCommands> {
    let input = input.trim();

    // expect like "bTICKER@100@250" or "sTICKER@50@120"
    let (side, rest) = input.split_at(1);

    // a cancel order, of the form X<order_id>, i.e. X123
    if side == "X" {
        let order_id: u16 = rest.parse::<u16>().ok()?;
        if let Ok(mut index) = order_index.lock() {
            if let Some((plevel, kind, sym)) = index.get_info_about_order(order_id) {
                eprintln!("cancelling {} {:?} {:?}", plevel, kind, &sym);
                index.remove_order(order_id); // TODO: this should really be called once server sigs back cancel complete. but leave it here so we can free up slots temporarily.
                drop(index);
                return Some(ClientCommands::Cancel(OrderCancel {
                    client_id: Uuid::default(), // default filler, it's set server side.
                    sym: sym,
                    price_level: plevel,
                    kind: kind,
                    order_id: order_id,
                }));
            } else {
                return None;
            }
        }
    }

    let mut parts = rest.split('@');
    let sym: &str = parts.next()?;
    let quantity_str: &str = parts.next()?;
    let price_str: &str = parts.next()?;

    let quantity = quantity_str.parse::<usize>().ok()?;
    let price = price_str.parse::<i64>().ok()?;

    let kind = match side {
        "b" => Some(OrderType::MarketBuy),
        "s" => Some(OrderType::MarketSell),
        "B" => Some(OrderType::LimitBuy),
        "S" => Some(OrderType::LimitSell),
        _ => None,
    };

    Some(ClientCommands::New(Order::new(
        None,
        Symbol(sym.into()),
        quantity,
        price,
        kind,
    )))
}

#[allow(unused)]
async fn read_stdin(tx: mpsc::UnboundedSender<Message>) {
    let mut stdin = tokio::io::stdin();
    loop {
        let mut buf = vec![0; 1024];
        let n = match stdin.read(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);
        tx.send(Message::binary(buf)).unwrap();
    }
}

async fn read_stdin_orders(
    tx: mpsc::UnboundedSender<Message>,
    order_index: Arc<Mutex<OrderIndex>>,
) {
    let mut stdin = tokio::io::stdin();
    let mut buf = Vec::with_capacity(1024);

    loop {
        buf.clear();
        let n = match stdin.read_buf(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };

        let s = String::from_utf8_lossy(&buf[..n]);
        for line in s.lines() {
            if let Some(client_cmds) = parse_order(line, order_index.clone()) {
                let bytes = Bytes::from(&client_cmds);
                tx.send(Message::Binary(bytes)).unwrap();
            }
        }
    }
}

async fn random_spawn_orders(
    tx: mpsc::UnboundedSender<Message>,
    market_prices: Arc<DashMap<Symbol, PriceTime>>,
    order_index: Arc<Mutex<OrderIndex>>,
) {
    const TICKER_CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const ACTIONS: &[u8] = b"bBsS";

    let mut rng = rand::rng();
    let price_noise = Normal::new(0.0, 2.0).unwrap();
    let mut c: usize = 0;
    const BATCHSIZE: usize = 1 << 16;

    let start = std::time::Instant::now();

    loop {
        let action: char = ACTIONS[rng.random_range(0..ACTIONS.len())] as char;
        let ticker: String = (0..3)
            .map(|_| {
                let i: usize = rng.random_range(0..TICKER_CHARSET.len());
                TICKER_CHARSET[i] as char
            })
            .collect();

        let quantity = rng.random_range(25..=250);
        let sym = Symbol(Arc::from(ticker.clone()));

        // TODO: better logic for initial price
        // Magic number for now.
        let cp = if let Some(pt) = market_prices.get(&sym) {
            (*pt).0
        } else {
            150
        };
        let price = cp + (price_noise.sample(&mut rng) as i64);

        let cmd: String = format!("{}{}@{}@{}", action, ticker, quantity, price);

        if let Some(client_cmds) = parse_order(&cmd, order_index.clone()) {
            c += 1;
            if c & (BATCHSIZE - 1) == 0 {
                c = 0;
                println!("ts: {:?} - submitted {BATCHSIZE} orders", start.elapsed());
            }
            let bytes = Bytes::from(&client_cmds);
            tx.send(Message::Binary(bytes)).unwrap();
        }
    }
}

struct OrderIndex {
    id_to_plevel: Vec<i64>,
    id_to_type: Vec<u8>,
    valid: Vec<bool>,
    id_to_sym: Vec<Option<Symbol>>,
}

impl OrderIndex {
    fn new() -> Self {
        Self {
            id_to_plevel: vec![0_i64; MAX_ACTIVE_ORDERS],
            id_to_type: vec![0_u8; MAX_ACTIVE_ORDERS],
            valid: vec![false; MAX_ACTIVE_ORDERS],
            id_to_sym: (0..MAX_ACTIVE_ORDERS).map(|_| None).collect(),
        }
    }

    fn get_info_about_order(&self, order_id: u16) -> Option<(i64, OrderType, Symbol)> {
        let idx: usize = order_id as usize;
        if !self.valid[idx] {
            return None;
        }
        let kind = self.map_int_to_order_type(self.id_to_type[idx]);
        if kind.is_none() {
            return None;
        }
        let sym: Symbol = self.id_to_sym[idx].clone().unwrap(); // let it panic here, as sym should always exist by the time we get here.
        Some((self.id_to_plevel[idx], kind.unwrap(), sym))
    }

    fn index_order(&mut self, or: OrderReceived) {
        let idx: usize = or.order_id as usize;
        if self.valid[idx] {
            eprintln!("order already exists in slot {}, skipping...", idx);
        }
        self.id_to_plevel[idx] = or.price;
        self.id_to_type[idx] = self.map_order_type_to_idx(or.kind);
        self.id_to_sym[idx] = Some(or.sym.clone());
        self.valid[idx] = true;
    }

    fn remove_order(&mut self, order_id: u16) {
        let idx: usize = order_id as usize;
        self.valid[idx] = false;
    }

    fn map_int_to_order_type(&self, v: u8) -> Option<OrderType> {
        match v {
            1 => Some(OrderType::MarketBuy),
            2 => Some(OrderType::MarketSell),
            3 => Some(OrderType::LimitBuy),
            4 => Some(OrderType::LimitSell),
            _ => None,
        }
    }

    fn map_order_type_to_idx(&self, ot: OrderType) -> u8 {
        match ot {
            OrderType::MarketBuy => 1,
            OrderType::MarketSell => 2,
            OrderType::LimitBuy => 3,
            OrderType::LimitSell => 4,
        }
    }
}

#[tokio::main]
async fn main() {
    let url = env::args()
        .nth(1)
        .unwrap_or_else(|| panic!("requires >= one argument"));

    let auto_client: bool = env::args().any(|a| a == "--auto");

    let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
    let stdin_rx = UnboundedReceiverStream::new(stdin_rx);

    let market_prices: Arc<DashMap<Symbol, PriceTime>> = Arc::new(DashMap::new());
    let order_index: Arc<Mutex<OrderIndex>> = Arc::new(Mutex::new(OrderIndex::new()));

    if auto_client {
        tokio::spawn(random_spawn_orders(
            stdin_tx,
            market_prices.clone(),
            order_index.clone(),
        ));
    } else {
        tokio::spawn(read_stdin_orders(stdin_tx, order_index.clone()));
    }

    let (ws_stream, _) = connect_async(&url).await.expect("failed to connect");
    println!("websocket handshake success!");

    let (write, read) = ws_stream.split();

    let stdin_to_ws = stdin_rx.map(Ok).forward(write);

    let ws_to_stdout = {
        read.for_each(|message| async {
            match message {
                Ok(Message::Binary(data)) => match bincode::deserialize::<Frame>(&data)
                {
                    Ok(Frame::Order(_o)) => {} //eprintln!("received {:?}", o) }
                    Ok(Frame::OrderReceived(or)) => {
                        if let Ok(mut index) = order_index.lock() {
                            index.index_order(or);
                        }
                    }
                    Ok(Frame::Prices(spv)) => {
                        for sp in spv.prices.iter() {
                            if let Some(p) = sp.price {
                                let pt: PriceTime = (p, sp.dt);
                                market_prices.insert(sp.sym.clone(), pt);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("decoding failed {:?}", e);
                    }
                },
                Err(e) => {
                    eprintln!("decodnign failed, outer {:?}", e);
                }
                _ => {
                    eprintln!("no matches {:?}", message);
                }
            }
        })
    };

    pin_mut!(stdin_to_ws, ws_to_stdout);
    tokio::select! { _ = stdin_to_ws => {}, _ = ws_to_stdout => {} }
}
