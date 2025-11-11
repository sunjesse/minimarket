use anyhow::Result;
use bytes::Bytes;
use rayon::{prelude::*, ThreadPoolBuilder};
use serde_json;
use std::{env, io::Error as IoError, sync::Arc};
use tokio::{
    net::{TcpStream, TcpListener},
    sync::mpsc,
};

mod utils;
use utils::{binary_insert_by_key, rayon_await};

mod matcher;
use matcher::{Book, Order};

mod connection;
use connection::{new_connection, Hub, Processor};

async fn sequencer(
    hub: Arc<Hub>,
    mut rx: mpsc::Receiver<Bytes>,
    tx: mpsc::Sender<Bytes>, // -> matcher
) {
    while let Some(x) = rx.recv().await {
        println!(
            "seq has received {:?}, tid: {:?}",
            x,
            std::thread::current().id()
        );
        if tx.send(x.clone()).await.is_err() {
            println!("ERROR'd SENDING {:?} from SEQ -> MAT", x);
        }
    }
}

async fn matcher(hub: Arc<Hub>, pool: Arc<rayon::ThreadPool>, mut rx: mpsc::Receiver<Bytes>) {
    while let Some(x) = rx.recv().await {
        println!(
            "MAT RECEIVED {:?}, tid: {:?}",
            x,
            std::thread::current().id()
        );
        let s = rayon_await(pool.clone(), || {
            // TODO: fill with matching task
            println!("tid rayon: {:?}", std::thread::current().id());
            (0u64..50_000_000).into_par_iter().sum::<u64>()
        })
        .await;
        println!("done {:?}", s);
    }
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    let addr: String = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let hub: Arc<Hub> = Hub::new();

    let listener = TcpListener::bind(&addr).await.expect("bind failed");
    println!("listening on: {}", addr);

    let (seq_tx, seq_rx) = mpsc::channel::<Bytes>(1024);
    let (mat_tx, mat_rx) = mpsc::channel::<Bytes>(1024);

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(num_cpus::get_physical())
            .thread_name(|i| format!("thread-{}", i))
            .build()
            .unwrap(),
    );

    // spawn sequencer task
    tokio::spawn(sequencer(hub.clone(), seq_rx, mat_tx));
    tokio::spawn(matcher(hub.clone(), pool.clone(), mat_rx));

    let proc: Arc<Processor> = Arc::new(Processor::new(hub.clone(), seq_tx));

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(new_connection(proc.clone(), stream, addr));
    }

    Ok(())
}
