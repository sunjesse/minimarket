use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::{
    env,
    sync::{Arc, mpsc as smpsc},
};
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
            eprintln!("[sequencer] matcher channel closed, shutting down...");
            break;
        }
    }
}

async fn matcher(
    mut rx: mpsc::Receiver<TimedOrder>,
    shards: Vec<smpsc::SyncSender<TimedOrder>>,
) {
    let n = shards.len();
    while let Some(to) = rx.recv().await {
        let i = shard_for(&to.order.sym, n);
        if let Err(e) = shards[i].try_send(to) {
            eprintln!("[matcher] shard {} errored with {:}", i, e);
        }
    }
}

async fn broadcaster(hub: Arc<Hub>, mut rx: mpsc::Receiver<Arc<Vec<Order>>>) {
    while let Some(x) = rx.recv().await {
        hub.broadcast_to(x.clone());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr: String = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_string());

    let hub: Arc<Hub> = Hub::new();

    let listener = TcpListener::bind(&addr).await?;
    println!("listening on: {}", addr);

    let (seq_tx, seq_rx) = mpsc::channel::<Order>(1024);
    let (mat_tx, mat_rx) = mpsc::channel::<TimedOrder>(1024);
    let (bc_tx, bc_rx) = mpsc::channel::<Arc<Vec<Order>>>(1024);

    let nshards = (num_cpus::get_physical() - 1).max(1);
    println!("num matcher shards: {}", nshards);

    let global_prices: Arc<DashMap<Symbol, SecPrice>> = Arc::new(DashMap::new());

    let shards = Shard::spawn_shards(nshards, bc_tx, global_prices.clone());

    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(mat_rx, shards));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));

    // current market prices broadcaster task
    {
        let hub = hub.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let sec_prices = list_all_security_prices(global_prices.clone());
                let prices = Arc::new(Bytes::from(&Frame::Prices(sec_prices)));
                hub.broadcast(prices);
            }
        });
    }

    /*
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
    */

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tokio::spawn(conn_task(hub.clone(), seq_tx.clone(), stream, addr));
            }
            Err(e) => {
                eprintln!("restarting, due to error {:?}", e)
            }
        }
    }
}
