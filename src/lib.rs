use anyhow::anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{copy, read_dir, read_to_string, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use ulid::Ulid;

const MAX_BYTE_SIZE: u64 = 5_242_880;
const TOMBSTONE_VALUE: &str = "__TOMBSTONE__";

#[derive(Serialize, Deserialize, Debug)]
pub struct Pair<'a> {
    pub key: &'a str,
    pub keysize: usize,
    pub value: &'a str,
    pub value_size: usize,
    timestamp: SystemTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeydirVal {
    entry_size: usize,
    entry_pos: u64,
    file_id: PathBuf,
    timestamp: SystemTime,
}

pub struct Bitcask {
    directory: Box<Path>,
    filepath: PathBuf,
    file: File,
    offset: usize, //stores the content of the file when opened. helps guide insertion.
    keydir: HashMap<String, KeydirVal>,
}

fn build_keydir(dir: &Path) -> Result<HashMap<String, KeydirVal>> {
    let paths = read_dir(dir)?;
    let mut pos = 0;
    let mut keydir = HashMap::new();

    for file in paths {
        let file = file?;
        let content = read_to_string(file.path())?;
        if content.is_empty() {
            continue;
        }
        for line in content.split('\n') {
            if line.is_empty() {
                break;
            }
            let pair: Pair = serde_json::from_str(line)?;
            if pair.value == TOMBSTONE_VALUE {
                keydir.remove(pair.key);
                continue;
            }
            let kdirval = KeydirVal {
                entry_size: line.len(),
                entry_pos: pos,
                file_id: file.path(),
                timestamp: pair.timestamp,
            };
            pos += line.len() as u64 + 1;
            match keydir.get(pair.key) {
                None => {
                    keydir.insert(pair.key.to_string(), kdirval);
                }
                Some(val) => {
                    if pair.timestamp > val.timestamp {
                        keydir.insert(pair.key.to_string(), kdirval);
                    }
                }
            }
        }
    }

    Ok(keydir)
}

impl Bitcask {
    pub fn open(dir: &Path) -> Result<Bitcask> {
        let filepath = dir.join(Path::new("activelog.btk"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&filepath)?;

        Ok(Bitcask {
            directory: dir.into(),
            file,
            offset: read_to_string(&filepath)?.len(),
            filepath,
            keydir: build_keydir(dir)?,
        })
    }

    fn dropcurrentfile(&mut self) -> Result<()> {
        let newfilename = Ulid::new().to_string() + ".btk";
        let path = self.directory.join(Path::new(&newfilename));

        copy(self.filepath.clone(), path)?;
        self.file.set_len(0)?;

        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        match self.get(key.to_string())? {
            Some(_) => {
                let time = SystemTime::now();
                let pair = serde_json::to_string(&Pair {
                    key,
                    keysize: key.len(),
                    value: TOMBSTONE_VALUE,
                    value_size: TOMBSTONE_VALUE.len(),
                    timestamp: time,
                })?;

                let _ = self.file.write(pair.as_bytes())?;
                let _ = self.file.write("\n".as_bytes())?;
                self.keydir.remove(key);
                Ok(())
            }
            None => Err(anyhow!("key does not exist in the database".to_string())),
        }
    }

    pub fn put<'a>(&mut self, key: &'a str, value: &'a str) -> Result<()> {
        let time = SystemTime::now();
        let pair = serde_json::to_string(&Pair {
            key,
            keysize: key.len(),
            value,
            value_size: value.len(),
            timestamp: time,
        })?;

        let cursor = self.file.stream_position()?;
        let mut pos = std::cmp::max(cursor, self.offset as u64);

        // check if file is larger than 5mb
        if pos >= MAX_BYTE_SIZE - pair.len() as u64 {
            self.dropcurrentfile()?;
            pos = 0;
        };
        let _ = self.file.write(pair.as_bytes())?;
        let _ = self.file.write("\n".as_bytes())?;
        let kdirval = KeydirVal {
            entry_size: pair.len(),
            entry_pos: pos,
            file_id: self.filepath.clone(),
            timestamp: time,
        };
        self.keydir.insert(key.to_owned(), kdirval);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.sync()?;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.keydir.get(&key).cloned() {
            Some(v) => {
                let mut buf = vec![0u8; v.entry_size];
                let mut f = OpenOptions::new().read(true).open(v.file_id)?;
                f.seek(SeekFrom::Start(v.entry_pos))?;
                f.read_exact(&mut buf)?;
                let jstr = String::from_utf8(buf)?;
                let pair: Pair = serde_json::from_str(jstr.as_str())?;
                Ok(Some(pair.value.to_string()))
            }
            None => Ok(None),
        }
    }

    pub fn list_keys(&mut self) -> Vec<&String> {
        let keys_iter: Vec<&String> = Vec::from_iter(self.keydir.keys());
        keys_iter
    }

    //Merge several data files within a Bitcask datastore into a more
    //compact form. Also, produce hintfiles for faster startup.
    #[allow(dead_code)]
    fn merge(&self) -> Result<()> {
        let paths = read_dir(self.directory.clone())?;
        let mut non_active_files: Vec<PathBuf> = Vec::new();
        for path in paths {
            let p = path?;
            if p.path() != self.filepath {
                non_active_files.push(p.path());
            }
        }
        Ok(())
    }
}
