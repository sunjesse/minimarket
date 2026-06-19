use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::fs;
use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::PathBuf,
};

#[derive(Debug)]
pub struct SnapshotJob {
    snapshot_dir: PathBuf,
}

impl SnapshotJob {
    pub fn new() -> Self {
        Self {
            snapshot_dir: PathBuf::from("./snapshots"),
        }
    }

    pub fn save<T: Serialize>(&self, payload: T) -> Result<()> {
        let fname = self.snapshot_dir.join("state.dat");

        fs::create_dir_all(&self.snapshot_dir)?;

        let encoded: Vec<u8> = bincode::serialize(&payload)?;

        // length delimeter
        let len_bytes = (encoded.len() as u64).to_le_bytes();

        let mut file = OpenOptions::new().create(true).append(true).open(fname)?;

        file.write_all(&len_bytes)?;
        file.write_all(&encoded)?;
        file.flush()?;
        Ok(())
    }

    pub fn load_last<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        let fname = self.snapshot_dir.join("state.dat");

        let mut file = match File::open(fname) {
            Ok(f) => f,
            Err(ref e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut snapshot: Option<T> = None;

        loop {
            let mut len_buffer = [0u8; 8];

            match file.read_exact(&mut len_buffer) {
                Ok(()) => {
                    let payload_len = u64::from_le_bytes(len_buffer) as usize;
                    let mut payload_buffer = vec![0u8; payload_len];
                    file.read_exact(&mut payload_buffer)?;
                    snapshot = Some(bincode::deserialize(&payload_buffer)?);
                }
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(snapshot)
    }
}
