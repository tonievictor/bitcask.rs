use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{copy, read_dir, read_to_string, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Serialize, Deserialize)]
pub struct Pair {
    pub key: String,
    pub keysize: usize,
    pub value: String,
    pub value_size: usize,
    timestamp: SystemTime,
}

#[derive(Clone, Debug)]
pub struct KeydirVal {
    value_size: usize,
    value_pos: u64,
    timestamp: SystemTime,
}

pub struct Bitcask {
    directory: Box<Path>,
    filepath: PathBuf,
    file: File,
    cursor: usize,
    keydir: HashMap<String, KeydirVal>,
}

impl Bitcask {
    pub fn open(directory: &Path, filepath: impl Into<PathBuf>) -> Result<Bitcask> {
        let path = filepath.into();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let contents = read_to_string(path.clone())?;

        Ok(Bitcask {
            directory: directory.into(),
            file,
            cursor: contents.as_str().as_bytes().len(),
            filepath: path,
            keydir: HashMap::new(),
        })
    }

    //Merge several data files within a Bitcask datastore into a more
    //compact form. Also, produce hintfiles for faster startup.
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

    fn dropcurrentfile(&mut self) -> Result<()> {
        let newfilename = Ulid::new().to_string() + ".btk";
        let path = self.directory.join(Path::new(&newfilename));

        copy(self.filepath.clone(), path)?;
        remove_file(self.filepath.clone())?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.filepath.clone())?;

        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let pair = serde_json::to_string(&Pair {
            key: String::from(key),
            keysize: key.len(),
            value: String::from(value),
            value_size: value.len(),
            timestamp: SystemTime::now(),
        })?;

        // if the file is bigger than or close to 5mb,
        let cursor = self.file.stream_position()?;
        let mut pos = if cursor >= self.cursor as u64 {
            cursor
        } else {
            self.cursor as u64
        };

        if pos >= 5120 - pair.len() as u64 {
            self.dropcurrentfile()?;
            pos = 0;
        };
        let _ = self.file.write(pair.as_bytes())?;
        let kdirval = KeydirVal {
            value_size: pair.len(),
            value_pos: pos,
            timestamp: SystemTime::now(),
        };
        let _oldval = self.keydir.insert(key.to_owned(), kdirval);
        Ok(())
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
}
