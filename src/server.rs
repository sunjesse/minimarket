use anyhow::Result;
use bytes::Bytes;
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
            exchange_arc.add_order(to)
        })
        .await;
        println!("[matcher] done {:?}", s);
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
            .num_threads((ncpus - 1).max(1)) // save one for tokio
            .thread_name(|i| format!("thread-{}", i))
            .build()
            .unwrap(),
    );

    let exchange = Arc::new(Exchange::new(bc_tx));

    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(hub.clone(), pool.clone(), mat_rx, exchange.clone()));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));

    // current market prices broadcaster task
    {
        let hub = hub.clone();
        let exchange = exchange.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let sec_prices = exchange.list_all_security_prices();
                let prices = Arc::new(Bytes::from(&Frame::Prices(sec_prices)));
                hub.broadcast(prices);
            }
        });
    }

    // snapshot job
    let snapshot = Arc::new(SnapshotJob::new());
    {
        let snapshot = snapshot.clone();
        let exchange = exchange.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let sec_prices = exchange.list_all_security_prices();
                let snapshot = snapshot.clone();
                match tokio::task::spawn_blocking(move || snapshot.save(sec_prices))
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => eprintln!("snapshot save failed {:?}", e),
                    Err(e) => eprintln!("snapshot save panicked {:?}", e),
                }
            }
        });
    }

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(conn_task(hub.clone(), seq_tx.clone(), stream, addr));
    }

    Ok(())
}
