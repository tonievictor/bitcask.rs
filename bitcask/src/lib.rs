use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use ulid::Ulid;

pub struct Bitcask {
    file: File,
    keydir: HashMap<String, KeydirVal>,
}

#[derive(Serialize, Deserialize)]
pub struct Pair {
    pub key: String,
    pub keysize: usize,
    pub value: String,
    pub value_size: usize,
    timestamp: SystemTime,
}

#[derive(Clone)]
pub struct KeydirVal {
    //file_id: String,
    value_size: usize,
    value_pos: u64,
    timestamp: SystemTime,
}

impl Bitcask {
    pub fn open(directory: impl Into<PathBuf>) -> Result<Bitcask> {
        let dir = directory.into();
        let filename = Ulid::new().to_string();
        let path = dir.join(Path::new(&filename));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Bitcask {
            //path,
            file,
            keydir: HashMap::new(),
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let pair = Pair {
            key: String::from(key),
            keysize: key.len(),
            value: String::from(value),
            value_size: value.len(),
            timestamp: SystemTime::now(),
        };

        let pos = self.file.stream_position()?;
        let p = serde_json::to_string(&pair)?;
        let _ = self.file.write(p.as_bytes())?;
        let kdirval = KeydirVal {
            value_size: p.len(),
            value_pos: pos,
            timestamp: SystemTime::now(),
        };

        match self.keydir.insert(key.to_owned(), kdirval) {
            Some(_) => Ok(()),
            None => Err(anyhow!("Unable to write to keydir")),
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.keydir.get(&key).cloned() {
            Some(v) => {
                let mut buf = vec![0u8; v.value_size];
                self.file.seek(SeekFrom::Start(v.value_pos))?;
                self.file.read_exact(&mut buf)?;
                let jstr = String::from_utf8(buf)?;
                let pair: Pair = serde_json::from_str(jstr.as_str())?;
                Ok(Some(pair.value))
            }
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, _key: String) -> Result<()> {
        Ok(())
    }
}
