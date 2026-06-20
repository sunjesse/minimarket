use dashmap::DashMap;
use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, mpsc as smpsc},
    thread::Builder as ThreadBuilder,
};
use tokio::sync::mpsc;

use crate::*;

pub struct Shard {
    securities: HashMap<Symbol, Security>,
    broadcast_tx: mpsc::Sender<Arc<Vec<Order>>>,
}

impl Shard {
    pub fn add_order(&mut self, to: TimedOrder) -> Option<Order> {
        let sec = self
            .securities
            .entry(to.order.sym.clone())
            .or_insert_with(|| {
                Security::new(to.order.sym.clone(), self.broadcast_tx.clone())
            });

        match to.order.kind {
            Some(OrderType::MarketBuy) => sec.buy_nowait(to),
            Some(OrderType::MarketSell) => sec.sell_nowait(to),
            Some(OrderType::LimitSell) => sec.sell_wait(to),
            Some(OrderType::LimitBuy) => sec.buy_wait(to),
            None => None,
        }
    }

    pub fn spawn_shards(
        n: usize,
        bc_tx: mpsc::Sender<Arc<Vec<Order>>>,
        global_prices: Arc<DashMap<Symbol, SecPrice>>,
        snapshot: Arc<SnapshotJob>,
    ) -> Vec<smpsc::SyncSender<TimedOrder>> {
        (0..n)
            .map(|i| {
                let (tx, rx) = smpsc::sync_channel::<TimedOrder>(1024);
                let bc_tx = bc_tx.clone();
                let global_prices = global_prices.clone();
                let snapshot = snapshot.clone();
                ThreadBuilder::new()
                    .name(format!("shard-{i}"))
                    .spawn(move || {
                        let mut shard = Shard {
                            securities: HashMap::new(),
                            broadcast_tx: bc_tx,
                        };
                        let mut it: usize = 0;
                        while let Ok(to) = rx.recv() {
                            let sym = to.order.sym.clone();
                            let _ = shard.add_order(to);
                            it += 1;
                            if let Some(sec) = shard.securities.get(&sym) {
                                global_prices.insert(
                                    sym.clone(),
                                    SecPrice::new(sym, sec.current_price()),
                                );
                            }
                            if (it & 65535) == 0 {
                                // i % 2^16
                                it = 0;
                                let book: Vec<&Security> =
                                    shard.securities.values().collect();
                                let _ = snapshot.save(book, &format!("shard-{i}"));
                            }
                        }
                    })
                    .unwrap();
                tx
            })
            .collect()
    }
}

pub fn shard_for(symbol: &Symbol, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    symbol.hash(&mut h);
    let u = (h.finish() % n as u64) as usize;
    u
}

pub fn list_all_security_prices(
    global_prices: &DashMap<Symbol, SecPrice>,
) -> SecPriceVec {
    let p: Vec<SecPrice> = global_prices.iter().map(|kv| kv.value().clone()).collect();
    SecPriceVec { prices: p }
}
