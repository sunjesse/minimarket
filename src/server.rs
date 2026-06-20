use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc as smpsc,
    },
    time::Duration,
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
    completed: Arc<AtomicU64>,
) {
    let n = shards.len();
    while let Some(to) = rx.recv().await {
        let i = shard_for(&to.order.sym, n);
        if let Err(e) = shards[i].try_send(to) {
            eprintln!("[matcher] shard {} errored with {:}", i, e);
        } else {
            completed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn broadcaster(hub: Arc<Hub>, mut rx: mpsc::Receiver<Arc<Vec<Order>>>) {
    while let Some(x) = rx.recv().await {
        hub.broadcast_to(x.clone());
    }
}

async fn periodic_snapshot(
    hub: Arc<Hub>,
    snapshot: Arc<SnapshotJob>,
    global_prices: Arc<DashMap<Symbol, SecPrice>>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let sec_prices = list_all_security_prices(&global_prices);
        let prices = Arc::new(Bytes::from(&Frame::Prices(sec_prices.clone())));
        hub.broadcast(prices);

        let snapshot = snapshot.clone();
        match tokio::task::spawn_blocking(move || snapshot.save(sec_prices)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("snapshot save failed {:?}", e),
            Err(e) => eprintln!("snapshot save panicked {:?}", e),
        }
    }
}

async fn perf_profile(completed: Arc<AtomicU64>) {
    const INTERVAL_SECS: u64 = 2;
    let mut interval = tokio::time::interval(Duration::from_secs(INTERVAL_SECS));
    let mut prev = 0u64;
    loop {
        interval.tick().await;
        let now = completed.load(Ordering::Relaxed);
        eprintln!(
            "[matcher] completed {} orders/s",
            (now - prev) / INTERVAL_SECS
        );
        prev = now;
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

    let matcher_completed = Arc::new(AtomicU64::new(0));
    let snapshot = Arc::new(SnapshotJob::new());

    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(mat_rx, shards, matcher_completed.clone()));
    tokio::spawn(broadcaster(hub.clone(), bc_rx));
    tokio::spawn(periodic_snapshot(
        hub.clone(),
        snapshot.clone(),
        global_prices.clone(),
    ));
    tokio::spawn(perf_profile(matcher_completed.clone()));

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
