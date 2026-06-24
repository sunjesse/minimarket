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
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};

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
    completed: Arc<Vec<AtomicU64>>,
) {
    let n = shards.len();
    while let Some(to) = rx.recv().await {
        let i = shard_for(&to.order.sym, n);
        if let Err(e) = shards[i].try_send(to) {
            eprintln!("[matcher] shard {} errored with {:}", i, e);
        } else {
            completed[i].fetch_add(1, Ordering::Relaxed);
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
        // save state of market prices, but not the bid/asks. that is done inside
        // the impl of Shard
        match tokio::task::spawn_blocking(move || {
            snapshot.save(sec_prices, "market_prices")
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("snapshot save failed {:?}", e),
            Err(e) => eprintln!("snapshot save panicked {:?}", e),
        }
    }
}

async fn perf_profile(completed: Arc<Vec<AtomicU64>>) {
    const INTERVAL_SECS: u64 = 2;
    let mut interval = tokio::time::interval(Duration::from_secs(INTERVAL_SECS));
    let n = completed.len();
    let mut prev = vec![0u64; n];
    loop {
        interval.tick().await;
        let mut total = 0u64;
        let mut per_shard = Vec::with_capacity(n);
        for i in 0..n {
            let now = completed[i].load(Ordering::Relaxed);
            let rate = (now - prev[i]) / INTERVAL_SECS;
            per_shard.push(rate);
            total += rate;
            prev[i] = now;
        }
        eprintln!("[matcher] total {total} orders/s | per-shard {per_shard:?}");
    }
}

struct Server {
    addr: String,
    nshards: usize,
    task_set: JoinSet<()>,
}

impl Server {
    fn new(addr: String, matcher_nshards: usize) -> Self {
        Self {
            addr: addr,
            nshards: matcher_nshards,
            task_set: JoinSet::new(),
        }
    }

    async fn start_server(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("listening on: {}", self.addr);

        let hub: Arc<Hub> = Arc::new(Hub::new());
        let snapshot = Arc::new(SnapshotJob::new());

        let (seq_tx, seq_rx) = mpsc::channel::<Order>(1024);
        let (mat_tx, mat_rx) = mpsc::channel::<TimedOrder>(1024);
        let (bc_tx, bc_rx) = mpsc::channel::<Arc<Vec<Order>>>(1024);

        println!("num matcher shards: {}", self.nshards);

        let global_prices: Arc<DashMap<Symbol, SecPrice>> = Arc::new(DashMap::new());

        let shards = Shard::spawn_shards(
            self.nshards,
            bc_tx,
            global_prices.clone(),
            snapshot.clone(),
        );

        let matcher_completed: Arc<Vec<AtomicU64>> =
            Arc::new((0..self.nshards).map(|_| AtomicU64::new(0)).collect());

        self.task_set.spawn(sequencer(hub.clone(), seq_rx, mat_tx));
        self.task_set
            .spawn(matcher(mat_rx, shards, matcher_completed.clone()));
        self.task_set.spawn(broadcaster(hub.clone(), bc_rx));
        self.task_set.spawn(periodic_snapshot(
            hub.clone(),
            snapshot.clone(),
            global_prices.clone(),
        ));
        self.task_set.spawn(perf_profile(matcher_completed.clone()));

        loop {
            tokio::select! {
                // catch error state in the infinite loops
                Some(res) = self.task_set.join_next() => {
                    match res {
                        Ok(()) => eprintln!("[server] a task unexpected terminated"),
                        Err(e) => eprintln!("[server] a task panicked with {:?}", e),
                    }
                    // TODO: maybe rather than shutdown completely, somehow restart
                    // and pick up the last state before error? may require WALing...
                    eprintln!("[server] shutting down...");
                    self.task_set.shutdown().await;
                    std::process::exit(1);
                }

                // accept incoming ws connections and spawn conn_tasks.
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, addr)) => {
                            tokio::spawn(conn_task(hub.clone(), seq_tx.clone(), stream, addr));
                        }
                        Err(e) => {
                            eprintln!("restarting, due to error {:?}", e);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr: String = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8080".to_string());

    let matcher_nshards: usize = (num_cpus::get_physical() - 1).max(1);
    let mut server = Server::new(addr, matcher_nshards);
    server.start_server().await
}
