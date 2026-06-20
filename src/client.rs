use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{StreamExt, pin_mut};
use rand::prelude::*;
use rand_distr::Normal;
use std::{env, sync::Arc, time::SystemTime};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use minimarket::{Frame, Order, OrderType, Symbol};

type PriceTime = (i64, SystemTime);

fn parse_order(input: &str) -> Option<Order> {
    let input = input.trim();

    // expect like "bTICKER@100@250" or "sTICKER@50@120"
    if input.len() < 4 {
        return None;
    }

    let (side, rest) = input.split_at(1);
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

    Some(Order {
        id: None,
        sym: Symbol(sym.into()),
        quantity,
        price,
        kind,
    })
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

async fn read_stdin_orders(tx: mpsc::UnboundedSender<Message>) {
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
            if let Some(order) = parse_order(line) {
                let bytes = Bytes::from(&order);
                tx.send(Message::Binary(bytes)).unwrap();
            }
        }
    }
}

async fn random_spawn_orders(
    tx: mpsc::UnboundedSender<Message>,
    market_prices: Arc<DashMap<Symbol, PriceTime>>,
) {
    const TICKER_CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const ACTIONS: &[u8] = b"bBsS";

    let mut rng = rand::rng();
    let price_noise = Normal::new(0.0, 2.0).unwrap();
    let mut c: usize = 0;
    const BATCHSIZE: usize = 10_000;

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

        if let Some(order) = parse_order(&cmd) {
            //println!("[client] submitting order {:?}", order);
            c += 1;
            if c % BATCHSIZE == 0 {
                c = 0;
                println!("ts: {:?} - submitted {BATCHSIZE} orders", start.elapsed());
            }
            let bytes = Bytes::from(&order);
            tx.send(Message::Binary(bytes)).unwrap();
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

    if auto_client {
        tokio::spawn(random_spawn_orders(stdin_tx, market_prices.clone()));
    } else {
        tokio::spawn(read_stdin_orders(stdin_tx));
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
                    Ok(Frame::Order(_)) => {} //println!("received {:?}", o),
                    Ok(Frame::Prices(spv)) => {
                        for sp in spv.prices.iter() {
                            if let Some(p) = sp.price {
                                let pt: PriceTime = (p, sp.dt);
                                market_prices.insert(sp.sym.clone(), pt);
                            }
                        }
                    }
                    Err(_) => {}
                },
                Err(_) => {}
                _ => {}
            }
        })
    };

    pin_mut!(stdin_to_ws, ws_to_stdout);
    tokio::select! { _ = stdin_to_ws => {}, _ = ws_to_stdout => {} }
}
