use std::env;

use futures_util::{pin_mut, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

use minimarket::{order_to_bytes, Order, OrderType};

fn parse_order(input: &str) -> Option<Order> {
    let input = input.trim().to_lowercase();

    // expect like "b100@250" or "s50@120"
    if input.len() < 4 {
        return None;
    }

    let (side, rest) = input.split_at(1);
    let mut parts = rest.split('@');
    let quantity_str = parts.next()?;
    let price_str = parts.next()?;

    let quantity = quantity_str.parse::<usize>().ok()?;
    let price = price_str.parse::<f32>().ok()?;

    let kind = match side {
        "b" => Some(OrderType::MarketBuy),
        "s" => Some(OrderType::MarketSell),
        _ => None,
    };

    Some(Order {
        id: Uuid::new_v4(),
        quantity,
        price,
        kind,
    })
}

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
        .unwrap_or_else(|| panic!("this program requires at least one argument"));

    let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
    let stdin_rx = UnboundedReceiverStream::new(stdin_rx);

    tokio::spawn(read_stdin_orders(stdin_tx));

    let (ws_stream, _) = connect_async(&url).await.expect("Failed to connect");
    println!("websocket handshake has been successfully completed");

    let (write, read) = ws_stream.split();

    let stdin_to_ws = stdin_rx.map(Ok).forward(write);
    let ws_to_stdout = {
        read.for_each(|message| async {
            let data = message.unwrap().into_data();
            tokio::io::stdout().write_all(&data).await.unwrap();
        })
    };

    pin_mut!(stdin_to_ws, ws_to_stdout);
    tokio::select! { _ = stdin_to_ws => {}, _ = ws_to_stdout => {} }
}
