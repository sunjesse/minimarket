use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::fs;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::PathBuf,
};

const MAX_FILE_SIZE_BYTES: u64 = 1 << 26; // this is 64 MB

#[derive(Clone, Debug)]
pub struct SnapshotJob {
    snapshot_dir: PathBuf,
    version: HashMap<String, usize>,
}

impl SnapshotJob {
    pub fn new() -> Self {
        Self {
            snapshot_dir: PathBuf::from("./snapshots"),
            version: HashMap::new(),
        }
    }

    fn construct_fname(&self, name_ref: &str, v: usize) -> PathBuf {
        let fname = self.snapshot_dir.join(format!("{}-{}.dat", name_ref, v));
        fname
    }

    pub fn save<T: Serialize>(
        &mut self,
        payload: T,
        name: impl AsRef<str>,
    ) -> Result<()> {
        fs::create_dir_all(&self.snapshot_dir)?;

        let name_ref = name.as_ref();
        // local version count
        let mut v: usize = *self.version.entry(name_ref.to_string()).or_insert(0);
        let mut fname = self.construct_fname(name_ref, v);
        let mut file = OpenOptions::new().create(true).append(true).open(fname)?;

        // we loop to ensure on restarts, we find the latest version.
        while file.metadata()?.len() >= MAX_FILE_SIZE_BYTES {
            // each thread is pinned to a key in isolation
            // so we do not need this to be atomic
            // altho it does internally lock.
            v = *self
                .version
                .entry(name_ref.to_string())
                .and_modify(|v| *v += 1)
                .or_insert(0);
            fname = self.construct_fname(name_ref, v);
            file = OpenOptions::new().create(true).append(true).open(fname)?;
        }

        let encoded: Vec<u8> = bincode::serialize(&payload)?;

        // length delimeter
        let len_bytes = (encoded.len() as u64).to_le_bytes();

        file.write_all(&len_bytes)?;
        file.write_all(&encoded)?;

        // force a fsync syscall to ensure integrity
        file.sync_all()?;
        Ok(())
    }

    pub fn load_last<T: DeserializeOwned>(
        &self,
        name: impl AsRef<str>,
    ) -> Result<Option<T>> {
        let fname = self.snapshot_dir.join(format!("{}.dat", name.as_ref()));

        let mut file = match File::open(fname) {
            Ok(f) => f,
            Err(ref e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut snapshot: Option<T> = None;

        loop {
            let mut len_buffer = [0u8; 8];
            if let Err(e) = file.read_exact(&mut len_buffer) {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }

            let payload_len = u64::from_le_bytes(len_buffer) as usize;
            let mut payload_buffer = vec![0u8; payload_len];
            file.read_exact(&mut payload_buffer)?;
            snapshot = Some(bincode::deserialize(&payload_buffer)?);
        }
        Ok(snapshot)
    }
}
