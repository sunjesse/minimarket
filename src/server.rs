use anyhow::Result;
use bytes::Bytes;
use rayon::ThreadPoolBuilder;
use std::{
    env,
    io::Error as IoError,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, sync::mpsc};
use uuid::Uuid;

use minimarket::order::OrderType;
use minimarket::*;

async fn sequencer(
    _hub: Arc<Hub>,
    mut rx: mpsc::Receiver<Bytes>,
    tx: mpsc::Sender<Bytes>, // -> matcher
) {
    while let Some(x) = rx.recv().await {
        println!(
            "seq has received {:?}, tid: {:?}",
            x,
            std::thread::current().id()
        );
        if tx.send(x.clone()).await.is_err() {
            println!("ERROR'd SENDING {:?} from SEQ -> MAT", x);
        }
    }
}

async fn matcher(
    _hub: Arc<Hub>,
    pool: Arc<rayon::ThreadPool>,
    mut rx: mpsc::Receiver<Bytes>,
    book: Arc<Mutex<Book>>,
) {
    while let Some(x) = rx.recv().await {
        println!(
            "MAT RECEIVED {:?}, tid: {:?}",
            x,
            std::thread::current().id()
        );
        let book_arc = Arc::clone(&book);
        let s = rayon_await(pool.clone(), move || {
            // TODO: fill with matching task
            println!("tid rayon: {:?}", std::thread::current().id());
            book_arc.lock().unwrap().buy_nowait(300)
        })
        .await;
        println!("done {:?}", s);
    }
}

async fn broadcaster(hub: Arc<Hub>, mut rx: mpsc::Receiver<Arc<Vec<Order>>>) {
    while let Some(x) = rx.recv().await {
        println!("broadcasting {:?}", x);
        hub.broadcast_to(x.clone());
    }
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    let addr: String = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let hub: Arc<Hub> = Hub::new();

    let listener = TcpListener::bind(&addr).await.expect("bind failed");
    println!("listening on: {}", addr);

    let (seq_tx, seq_rx) = mpsc::channel::<Bytes>(1024);
    let (mat_tx, mat_rx) = mpsc::channel::<Bytes>(1024);
    let (bc_tx, bc_rx) = mpsc::channel::<Arc<Vec<Order>>>(1024);

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(num_cpus::get_physical() - 1) // save one for tokio
            .thread_name(|i| format!("thread-{}", i))
            .build()
            .unwrap(),
    );

    // book keeping
    let book = Arc::new(Mutex::new(Book::new(0, 0, hub.clone(), bc_tx)));

    {
        let mut l = book.lock().unwrap();
        l.sell_wait(Order {
            id: Uuid::new_v4(),
            quantity: 1000,
            price: 103_f32,
            kind: Some(OrderType::LimitSell),
        });

        l.sell_wait(Order {
            id: Uuid::new_v4(),
            quantity: 1000,
            price: 205_f32,
            kind: Some(OrderType::LimitSell),
        });

        l.buy_wait(Order {
            id: Uuid::new_v4(),
            quantity: 150,
            price: 100_f32,
            kind: Some(OrderType::LimitBuy),
        });
        println!("{:?}", l);
    }

    // spawn sequencer task
    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(hub.clone(), pool.clone(), mat_rx, book.clone()));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));

    let proc: Arc<Processor> = Arc::new(Processor::new(hub.clone(), seq_tx));

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(new_connection(proc.clone(), stream, addr));
    }

    Ok(())
}
