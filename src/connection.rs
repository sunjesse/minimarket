use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::{Frame, FreeList, Order};

#[derive(Clone, Debug)]
pub struct Conn {
    pub ctrl: mpsc::Sender<Message>,
    pub data: mpsc::Sender<Bytes>,
}

pub struct Hub {
    conns: DashMap<Uuid, Conn>,
    slots_per_conn: DashMap<Uuid, FreeList>,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            conns: DashMap::new(),
            slots_per_conn: DashMap::new(),
        }
    }

    pub fn register(self: &Arc<Self>, id: Uuid, out: Conn) {
        self.conns.insert(id, out);
        self.slots_per_conn.insert(id, FreeList::new());
    }

    pub fn unregister(&self, id: &Uuid) {
        self.conns.remove(id);
        self.slots_per_conn.remove(id);
    }

    pub fn claim_slot(&self, id: Uuid) -> Option<u16> {
        if let Some(mut free) = self.slots_per_conn.get_mut(&id) {
            return free.claim_slot();
        }
        None
    }

    pub fn drop_slot(&self, id: Uuid, slot_idx: u16) {
        if let Some(mut free) = self.slots_per_conn.get_mut(&id) {
            free.drop_slot(slot_idx);
        }
    }

    pub fn broadcast(&self, payload: Bytes) {
        for conn in self.conns.iter() {
            let _ = conn.data.try_send(payload.clone());
        }
    }

    pub fn broadcast_to(&self, orders: Vec<Order>) {
        for order in orders.iter() {
            if let Some(id) = order.get_client_id()
                && let Some(conn) = self.conns.get(&id)
            {
                // TODO: don't love this c.clone() here. figure out how to fix it.
                let _ = conn
                    .data
                    .try_send(Bytes::from(&Frame::Order(order.clone())).into());
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

    pub async fn send_data(&self, id: &Uuid, bytes: Bytes) -> Result<(), ()> {
        if let Some(conn) = self.conns.get(id) {
            conn.data
                .send(bytes)
                .await
                .map_err(|e| eprintln!("[hub] send_data error {:?}", e))
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
    let (tx_data, mut rx_data) = mpsc::channel::<Bytes>(512);

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
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                Some(msg) = rx_ctrl.recv() => {
                    if sink.send(msg).await.is_err() { break; }
                }
                Some(buf) = rx_data.recv() => {
                    if sink.send(Message::Binary(buf.into())).await.is_err() { break; }
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
                                eprintln!("I HAVE RECEIVED {:?}", b);
                                if let Some(order_id) = hub.claim_slot(client_id) {
                                    // TODO: this currently assumes b is valid order, else panics.
                                    let ord = Order::from(&b)
                                        .set_client_id(client_id)
                                        .set_order_id(order_id);

                                    if let Err(e) = seq_tx.send(ord.clone()).await {
                                        eprintln!("[connection] Errored with {}", e);
                                        break;
                                    } else {
                                    // the order is successfully sent to the sequencer
                                    // so we can sig back to the client the order_id.
                                        let _ = hub.send_data(
                                            &client_id,
                                            Bytes::from(
                                                &Frame::OrderReceived(ord.get_slim_view()
                                            ))
                                        ).await;
                                    }
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
