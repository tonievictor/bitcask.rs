use anyhow::Result;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use ulid::Ulid;

pub struct Bitcask {
    path: PathBuf,
    writer: BufWriter<File>,
    // the value will be a json serializable string
    reader: HashMap<String, String>,
}

impl Bitcask {
    pub fn open(directory: impl Into<PathBuf>) -> Result<Bitcask> {
        let dir = directory.into();
        let filename = Ulid::new().to_string();
        let path = dir.join(Path::new(&filename));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bw = BufWriter::new(file);
        Ok(Bitcask {
            path,
            writer: bw,
            reader: HashMap::new(),
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {}
    pub fn get(&self, key: String) -> Option<String> {}
    pub fn remove(&mut self, key: String) -> Result<()> {}
}
