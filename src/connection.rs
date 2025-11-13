use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::Order;

#[derive(Clone, Debug)]
pub struct Conn {
    pub ctrl: mpsc::Sender<Message>,
    pub data: mpsc::Sender<Arc<Bytes>>,
}

#[derive(Debug)]
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
        for conn in self.conns.iter() {
            let _ = conn.data.try_send(payload.clone());
        }
    }

    pub fn broadcast_to(&self, clients: Arc<Vec<Order>>) {
        for c in clients.iter() {
            if let Some(conn) = self.conns.get(&c.id) {
                let _ = conn.data.try_send(Bytes::from(c).into());
            }
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

pub async fn conn_task(
    hub: Arc<Hub>,
    seq_tx: mpsc::Sender<Order>,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    println!("socket up: {:?}", addr);

    let (tx_ctrl, mut rx_ctrl) = mpsc::channel::<Message>(64);
    let (tx_data, mut rx_data) = mpsc::channel::<std::sync::Arc<Bytes>>(512);

    let id = Uuid::new_v4();

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
                        let mut ord: Order = Order::from(&b);
                        ord.id = id; // TODO: figure out how to not need this later.
                        if seq_tx.send(ord).await.is_err() {
                            eprintln!("ERROR");
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
