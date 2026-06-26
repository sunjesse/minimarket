use std::sync::Mutex;

const MAX_ACTIVE_ORDERS: usize = u16::MAX as usize;
const N_CHUNKS: usize = MAX_ACTIVE_ORDERS / 64 + 1;

pub struct FreeList {
    free: Mutex<[u64; N_CHUNKS]>,
}

impl FreeList {
    pub fn new() -> Self {
        let mut free = [0_u64; N_CHUNKS];
        // occupy the first index 0, as 0 is unclaimable
        // order_id since we make order_id 0 as the default value.
        free[0] |= 1;
        Self {
            free: Mutex::new(free),
        }
    }

    pub fn claim_slot(&mut self) -> Option<u16> {
        if let Ok(mut free) = self.free.lock() {
            for (offset, x) in free.iter_mut().enumerate() {
                if *x != u64::MAX {
                    let i = x.trailing_ones() as usize;
                    *x |= 1 << i;
                    let slot: usize = offset * 64 + i;
                    return Some(slot as u16);
                }
            }
        }
        None
    }

    pub fn drop_slot(&mut self, slot_idx: u16) {
        let offset = (slot_idx / 64) as usize;
        let i = (slot_idx & 63) as usize;
        if let Ok(mut free) = self.free.lock() {
            free[offset] &= !(1 << i);
        }
    }
}
