use std::{cmp::Ordering, sync::Arc};
use tokio::sync::oneshot;

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
