use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::oneshot;

use crate::{Order, SecPriceVec};

pub async fn rayon_await<T: Send + 'static>(
    pool: Arc<rayon::ThreadPool>,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = oneshot::channel();
    pool.spawn(move || {
        let _ = tx.send(f());
    });
    rx.await.expect("rayon task panicked or pool dropped")
}

pub fn binary_insert_by_key<T, K: PartialOrd, F>(v: &mut Vec<T>, item: T, mut key: F, desc: bool)
where
    F: FnMut(&T) -> K,
{
    let item_key = key(&item);
    let pos = match v.binary_search_by(|x| {
        let cmp = key(x).partial_cmp(&item_key);
        if desc {
            cmp.map(Ordering::reverse)
        } else {
            cmp
        }
        .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        Ok(i) | Err(i) => i,
    };
    v.insert(pos, item);
}

#[derive(Serialize, Deserialize)]
pub enum Frame {
    Order(Order),
    Prices(SecPriceVec),
}

impl From<&Frame> for Bytes {
    fn from(f: &Frame) -> Self {
        Bytes::from(bincode::serialize(f).expect("s"))
    }
}

impl From<&Bytes> for Frame {
    fn from(b: &Bytes) -> Self {
        bincode::deserialize(b).expect("d")
    }
}
