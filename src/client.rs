use bytes::Bytes;
use futures_util::{StreamExt, pin_mut};
use rand::prelude::*;
use std::env;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use minimarket::{Frame, Order, OrderType, Symbol};

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

    println!("{:?}: {:?} shares @ {:?}", sym, quantity, price);
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

async fn random_spawn_orders(tx: mpsc::UnboundedSender<Message>) {
    const TICKER_CHARSET: &[u8] = b"ABC";
    const ACTIONS: &[u8] = b"bBsS";

    let mut rng = rand::rng();

    loop {
        let action: char = ACTIONS[rng.random_range(0..ACTIONS.len())] as char;
        let ticker: String = (0..3)
            .map(|_| {
                let i: usize = rng.random_range(0..TICKER_CHARSET.len());
                TICKER_CHARSET[i] as char
            })
            .collect();

        let quantity: String = rng.random_range(25..=250).to_string();

        // TODO: sample a normal distribution around each ticker's market price.
        let price: String = rng.random_range(150..=250).to_string();

        let cmd: String = format!("{}{}@{}@{}", action, ticker, quantity, price);

        if let Some(order) = parse_order(&cmd) {
            println!("VALID ORDER {:?}", order);

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

    let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
    let stdin_rx = UnboundedReceiverStream::new(stdin_rx);

    //tokio::spawn(read_stdin_orders(stdin_tx));
    tokio::spawn(random_spawn_orders(stdin_tx));

    let (ws_stream, _) = connect_async(&url).await.expect("failed to connect");
    println!("websocket handshake success!");

    let (write, read) = ws_stream.split();

    let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            match message {
                Ok(Message::Binary(data)) => match bincode::deserialize::<Frame>(&data) {
                    Ok(Frame::Order(o)) => println!("received {:?}", o),
                    Ok(Frame::Prices(p)) => println!("prices: {:?}", p),
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
