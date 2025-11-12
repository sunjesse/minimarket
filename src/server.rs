use anyhow::Result;
use rayon::ThreadPoolBuilder;
use std::{env, io::Error as IoError, sync::Arc};
use tokio::{net::TcpListener, sync::mpsc};

use minimarket::*;

async fn sequencer(
    _hub: Arc<Hub>,
    mut rx: mpsc::Receiver<Order>,
    tx: mpsc::Sender<Order>, // -> matcher
) {
    // TODO: make sequencer actually do something useful,
    // that is, giving strong ordering to the reqs coming in.
    while let Some(ord) = rx.recv().await {
        if tx.send(ord.clone()).await.is_err() {
            eprintln!("ERROR'd SENDING {:?} from SEQ -> MAT", ord);
        }
    }
}

async fn matcher(
    _hub: Arc<Hub>,
    pool: Arc<rayon::ThreadPool>,
    mut rx: mpsc::Receiver<Order>,
    exchange: Arc<Exchange>,
) {
    while let Some(ord) = rx.recv().await {
        let exchange_arc = Arc::clone(&exchange);
        let s = rayon_await(pool.clone(), move || {
            println!("ORDER IS {:?}", ord);
            exchange_arc.add_order(ord);
        })
        .await;
        println!("done {:?}", s);
    }
}

async fn broadcaster(hub: Arc<Hub>, mut rx: mpsc::Receiver<Arc<Vec<Order>>>) {
    while let Some(x) = rx.recv().await {
        hub.broadcast_to(x.clone());
    }
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    let addr: String = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_string());

    let hub: Arc<Hub> = Hub::new();

    let listener = TcpListener::bind(&addr).await.expect("bind failed");
    println!("listening on: {}", addr);

    let (seq_tx, seq_rx) = mpsc::channel::<Order>(1024);
    let (mat_tx, mat_rx) = mpsc::channel::<Order>(1024);
    let (bc_tx, bc_rx) = mpsc::channel::<Arc<Vec<Order>>>(1024);

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(num_cpus::get_physical() - 1) // save one for tokio
            .thread_name(|i| format!("thread-{}", i))
            .build()
            .unwrap(),
    );

    //let security = Arc::new(Mutex::new(Security::new("AAPL", bc_tx)));
    let exchange = Arc::new(Exchange::new(bc_tx));

    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(hub.clone(), pool.clone(), mat_rx, exchange.clone()));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));

    let proc: Arc<Processor> = Arc::new(Processor::new(hub.clone(), seq_tx));

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(conn_task(proc.clone(), stream, addr));
    }

    Ok(())
}
