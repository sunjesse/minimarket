use std::sync::Arc;
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

pub fn binary_insert_by_key<T, K: PartialOrd, F>(v: &mut Vec<T>, item: T, mut key: F)
where
    F: FnMut(&T) -> K,
{
    let item_key = key(&item);
    let pos = match v.binary_search_by(|x| {
        key(x)
            .partial_cmp(&item_key)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        Ok(i) | Err(i) => i,
    };
    v.insert(pos, item);
}
