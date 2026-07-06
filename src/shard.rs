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
    broadcast_tx: mpsc::Sender<Vec<Order>>,
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

    pub fn cancel_order(&mut self, oc: OrderCancel) -> bool {
        // return result?
        if let Some(sec) = self.securities.get_mut(&oc.sym) {
            sec.cancel_order_by_id(oc.client_id, oc.order_id, oc.price_level, oc.kind)
        } else {
            eprintln!("symbol doesn't exist {:?}", oc.sym);
            false
        }
    }

    pub fn spawn_shards(
        n: usize,
        bc_tx: mpsc::Sender<Vec<Order>>,
        global_prices: Arc<DashMap<Symbol, SecPrice>>,
        snapshot: SnapshotJob,
        load_from_checkpoint: bool,
        hub: Arc<Hub>,
    ) -> Vec<smpsc::SyncSender<ShardedOrder>> {
        (0..n)
            .map(|i| {
                let (tx, rx) = smpsc::sync_channel::<ShardedOrder>(1024);
                let bc_tx = bc_tx.clone();
                let global_prices = global_prices.clone();
                let shard_name: String = format!("shard-{i}");
                let mut snapshot = snapshot.clone();
                let hub = hub.clone();
                ThreadBuilder::new()
                    .name(shard_name.clone())
                    .spawn(move || {
                        let mut shard = Shard {
                            securities: HashMap::new(),
                            broadcast_tx: bc_tx.clone(),
                        };
                        if load_from_checkpoint {
                            match snapshot.load_last::<Vec<SecurityData>>(shard_name) {
                                Ok(Some(ds)) => {
                                    for d in ds {
                                        let sec = Security::from_data(d, bc_tx.clone());
                                        shard.securities.insert(sec.sym.clone(), sec);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("[shard-{i}] data load failed {:?}", e);
                                }
                                _ => {} // do nothing
                            }
                        }
                        let mut it: usize = 0;
                        while let Ok(so) = rx.recv() {
                            // if the ShardedOrder is a cancel order, just cancel the order.
                            // otherwise, we resolve this match to the TimedOrder and proceed with
                            // the matching algo.
                            let to = match so {
                                ShardedOrder::Cancel(oc) => {
                                    let client_id = oc.client_id;
                                    let order_id = oc.order_id;
                                    // TODO: right now we only check bool for success,
                                    // move to checking result?
                                    if shard.cancel_order(oc) {
                                        // drop the order slot if successfully cancelled.
                                        hub.drop_slot(client_id, order_id);
                                        eprintln!(
                                            "[shard] cancelled {} {}",
                                            client_id, order_id
                                        );
                                    } else {
                                        eprintln!(
                                            "[shard] order {} not cancelled",
                                            order_id
                                        );
                                    }
                                    continue;
                                }
                                ShardedOrder::Add(to) => to,
                            };

                            let sym = to.order.sym.clone();

                            let filled = shard.add_order(to);
                            // free up an order slot for filled order.
                            if let Some(fo) = filled {
                                let client_id = fo.get_client_id().unwrap(); // guaranteed to be Some at this point.
                                let order_id = fo.get_order_id();
                                hub.drop_slot(client_id, order_id);
                            }

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
                                let book: Vec<SecurityDataRef> = shard
                                    .securities
                                    .values()
                                    .map(|v| v.to_data_ref())
                                    .collect();
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
