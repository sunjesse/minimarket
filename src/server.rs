use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use rayon::{
    ThreadPoolBuilder,
    prelude::*,
};
use serde_json;
use std::{env, io::Error as IoError, net::SocketAddr, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Clone)]
pub struct Conn {
    pub ctrl: mpsc::Sender<Message>,
    pub data: mpsc::Sender<Arc<Bytes>>,
}

pub struct Hub {
    conns: DashMap<Uuid, Conn>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            conns: DashMap::new(),
        })
    }

    pub fn register(self: &Arc<Self>, id: Uuid, out: Conn) {
        self.conns.insert(id, out);
    }

    pub fn unregister(&self, id: &Uuid) {
        self.conns.remove(id);
    }

    pub fn broadcast(&self, payload: Arc<Bytes>) {
        for entry in self.conns.iter() {
            let _ = entry.data.try_send(payload.clone());
        }
    }

    pub async fn send_ctrl(&self, id: &Uuid, msg: Message) -> Result<(), ()> {
        if let Some(conn) = self.conns.get(id) {
            conn.ctrl.send(msg).await.map_err(|_| ())
        } else {
            Err(())
        }
    }
}

pub struct Processor {
    hub: Arc<Hub>,
    seq_tx: mpsc::Sender<Bytes>, // TODO: change type to struct
}

async fn new_connection(proc: Arc<Processor>, stream: TcpStream, addr: SocketAddr) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    println!("socket up: {:?}", addr);

    let (tx_ctrl, mut rx_ctrl) = mpsc::channel::<Message>(64);
    let (tx_data, mut rx_data) = mpsc::channel::<std::sync::Arc<Bytes>>(512);

    let id = Uuid::new_v4();
    let hub: Arc<Hub> = proc.hub.clone();
    let seq_tx = proc.seq_tx.clone(); // per connection sender

    hub.register(
        id,
        Conn {
            ctrl: tx_ctrl.clone(),
            data: tx_data.clone(),
        },
    );

    let (mut sink, mut source) = ws.split();

    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                Some(msg) = rx_ctrl.recv() => {
                    if sink.send(msg).await.is_err() { break; }
                }
                Some(buf) = rx_data.recv() => {
                    if sink.send(Message::Binary((*buf).clone())).await.is_err() { break; }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() { break; }
                }
            }
        }
        let _ = sink.close().await;
    });

    let reader = {
        let hub = hub.clone();
        tokio::spawn(async move {
            let mut last_pong = std::time::Instant::now();
            while let Some(msg) = source.next().await {
                match msg {
                    Ok(Message::Text(txt)) => {
                        println!("received {:?}", txt)
                    }
                    Ok(Message::Binary(b)) => {
                        println!("[{:?}] received bytes {:?}", addr, b);
                        if seq_tx.send(b).await.is_err() {
                            println!("ERROR");
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {
                        println!("[{:?}] ponged", addr);
                        last_pong = std::time::Instant::now();
                    }
                    Ok(Message::Ping(p)) => {
                        println!("[{:?}] pinged {:?}", addr, p);
                        let _ = hub.send_ctrl(&id, Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
                if last_pong.elapsed() > std::time::Duration::from_secs(60) {
                    break;
                }
            }
        })
    };

    tokio::select! { _ = writer => {}, _ = reader => {} }
    hub.unregister(&id);
    println!("[{:?}] disconnected", addr);
    Ok(())
}

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

#[derive(Copy, Clone)]
pub struct Order {
    quantity: usize,
    price: f32,
}

pub struct Book {
    id: usize,
    q: usize, // quantity
    bid: Vec<Order>, // sorted desc
    ask: Vec<Order>, // sorted asc 
}

impl Book {
    fn new(id: usize, quantity: usize) -> Self {
        Book {
            id: id,
            q: quantity,
            bid: Vec::new(),
            ask: Vec::new(),
        }
    }

    fn buy_nowait(&mut self, req_sz: usize) -> Option<Order> {
        if req_sz >= self.q { return None; }

        let mut c: usize = 0;
        let mut x: f32 = 0_f32;
        let mut i: usize = 0;

        for ord in self.ask.iter_mut() {
            // TODO: cleanup this implementation, not the cleanest
            let t: usize = ord.quantity + c;
            i += 1;
            if t >= req_sz {
                let diff: usize = t - req_sz;
                x += ord.price * (diff as f32); 
                c += diff;
                ord.quantity -= diff; 
                break;
            } else {
                x += ord.price * (ord.quantity as f32);
                c += ord.quantity;
            } 
        }

        if c < req_sz {
            return None;
        }

        // otherwise order was successful
        self.ask.drain(i..);
        Some(Order { quantity: c, price: x/(c as f32) }) 
    } 

    fn sell_wait(&mut self, order: Order) {
        binary_insert_by_key(&mut self.ask, order, |o| o.price);
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

// HELPERS
async fn rayon_await<T: Send + 'static>(
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

    let proc: Arc<Processor> = Arc::new(Processor {
        hub: hub.clone(),
        seq_tx: seq_tx,
    });

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(new_connection(proc.clone(), stream, addr));
    }

    Ok(())
}
