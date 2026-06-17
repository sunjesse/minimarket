use anyhow::Result;
use rayon::ThreadPoolBuilder;
use std::{env, io::Error as IoError, sync::Arc};
use tokio::{net::TcpListener, sync::mpsc};

use minimarket::*;

async fn sequencer(
    _hub: Arc<Hub>,
    mut rx: mpsc::Receiver<Order>,
    tx: mpsc::Sender<TimedOrder>, // -> matcher
) {
    while let Some(ord) = rx.recv().await {
        //println!("[sequencer] {:?}", ord);
        let to = TimedOrder::new(ord);
        if tx.send(to).await.is_err() {
            eprintln!("ERROR'd sending from SEQ -> MAT");
        }
    }
}

async fn matcher(
    _hub: Arc<Hub>,
    pool: Arc<rayon::ThreadPool>,
    mut rx: mpsc::Receiver<TimedOrder>,
    exchange: Arc<Exchange>,
) {
    while let Some(to) = rx.recv().await {
        let exchange_arc = Arc::clone(&exchange);
        let s = rayon_await(pool.clone(), move || {
            println!("[matcher] processing order {:?}", to);
            exchange_arc.add_order(to);
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
    let (mat_tx, mat_rx) = mpsc::channel::<TimedOrder>(1024);
    let (bc_tx, bc_rx) = mpsc::channel::<Arc<Vec<Order>>>(1024);

    let ncpus = num_cpus::get_physical();
    println!("num cpus: {}", ncpus);

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(ncpus - 1) // save one for tokio
            .thread_name(|i| format!("thread-{}", i))
            .build()
            .unwrap(),
    );

    let exchange = Arc::new(Exchange::new(bc_tx));

    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(hub.clone(), pool.clone(), mat_rx, exchange.clone()));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(conn_task(
            hub.clone(),
            exchange.clone(),
            seq_tx.clone(),
            stream,
            addr,
        ));
    }

    Ok(())
}
