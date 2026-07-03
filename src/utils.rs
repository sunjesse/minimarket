use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::{Order, OrderCancel, OrderReceived, SecPriceVec};

pub fn binary_insert_by_cmp<T, F>(v: &mut Vec<T>, item: T, mut cmp: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    let pos = match v.binary_search_by(|x| cmp(x, &item)) {
        Ok(i) | Err(i) => i,
    };
    // TODO: this is O(n)! we will move the bid/asks to
    // use queues instead.
    v.insert(pos, item);
}

#[derive(Serialize, Deserialize)]
pub enum Frame {
    Order(Order),
    OrderReceived(OrderReceived),
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

#[derive(Serialize, Deserialize)]
pub enum ClientCommands {
    New(Order),
    Cancel(OrderCancel),
}

impl From<&ClientCommands> for Bytes {
    fn from(cc: &ClientCommands) -> Self {
        Bytes::from(bincode::serialize(cc).expect("panik"))
    }
}

impl From<&Bytes> for ClientCommands {
    fn from(b: &Bytes) -> Self {
        bincode::deserialize(b).expect("panik")
    }
}
