use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::{Frame, Order};

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
            if let Some(id) = c.id
                && let Some(conn) = self.conns.get(&id)
            {
                // TODO: don't love this c.clone() here. figure out how to fix it.
                let _ = conn
                    .data
                    .try_send(Bytes::from(&Frame::Order(c.clone())).into());
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

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

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

    // this is client id
    let client_id = Uuid::new_v4();

    hub.register(
        client_id,
        Conn {
            ctrl: tx_ctrl.clone(),
            data: tx_data.clone(),
        },
    );

    let (mut sink, mut source) = ws.split();

    let mut writer = tokio::spawn(async move {
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

    let mut reader = {
        let hub = hub.clone();
        tokio::spawn(async move {
            let mut last_pong = tokio::time::Instant::now();
            loop {
                // race incoming frames against a heartbeat timer
                tokio::select! {
                    frame = source.next() => {
                        let Some(Ok(msg)) = frame else { break; };
                        match msg {
                            Message::Binary(b) => {
                                // TODO: this currently assumes b is valid order, else panics.
                                let mut ord = Order::from(&b);
                                ord.id = Some(client_id);
                                if let Err(e) = seq_tx.send(ord).await {
                                    eprintln!("[connection] Errored with {}", e);
                                    break;
                                }
                            }
                            Message::Pong(_) => {
                                last_pong = tokio::time::Instant::now();
                            }
                            Message::Ping(p) => {
                                let _ = hub.send_ctrl(&client_id, Message::Pong(p)).await;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    _ = tokio::time::sleep_until(last_pong + HEARTBEAT_TIMEOUT) => break,
                }
            }
        })
    };

    // abort the other task if one task fails to avoid accumulating zombie tasks
    tokio::select! {
        _ = &mut writer => { reader.abort(); }
        _ = &mut reader => { writer.abort(); }
    }

    hub.unregister(&client_id);
    println!("[{:?}] disconnected", addr);
    Ok(())
}
