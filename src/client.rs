use std::env;

use futures_util::{pin_mut, StreamExt};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

use minimarket::{bytes_to_order, order_to_bytes, Order, OrderType, Symbol};

fn parse_order(input: &str) -> Option<Order> {
    let input = input.trim();

    // expect like "b100@250" or "s50@120"
    if input.len() < 4 {
        return None;
    }

    let (side, rest) = input.split_at(1);
    let mut parts = rest.split('@');
    let sym: &str = parts.next()?;
    let quantity_str: &str = parts.next()?;
    let price_str: &str = parts.next()?;

    let quantity = quantity_str.parse::<usize>().ok()?;
    let price = price_str.parse::<f32>().ok()?;

    let kind = match side {
        "b" => Some(OrderType::MarketBuy),
        "s" => Some(OrderType::MarketSell),
        "B" => Some(OrderType::LimitBuy),
        "S" => Some(OrderType::LimitSell),
        _ => None,
    };

    println!("{:?}: {:?} shares @ {:?}", sym, quantity, price);
    Some(Order {
        id: Uuid::new_v4(), // TODO: replace with Option<Uuid> in future.
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
                let bytes = order_to_bytes(&order);
                tx.send(Message::Binary(bytes)).unwrap();
            }
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

    tokio::spawn(read_stdin_orders(stdin_tx));

    let (ws_stream, _) = connect_async(&url).await.expect("failed to connect");
    println!("websocket handshake success!");

    let (write, read) = ws_stream.split();

    let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            let data = message.unwrap().into_data();
            if data.len() > 0 {
                println!("received {:?}", bytes_to_order(&data));
            }
        })
    };

    pin_mut!(stdin_to_ws, ws_to_stdout);
    tokio::select! { _ = stdin_to_ws => {}, _ = ws_to_stdout => {} }
}
