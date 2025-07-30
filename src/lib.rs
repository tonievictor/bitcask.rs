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
pub struct Pair<'a> {
    pub key: &'a str,
    pub keysize: usize,
    pub value: &'a str,
    pub value_size: usize,
    timestamp: SystemTime,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
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
    cursor: usize, //stores the content of the file when opened. helps guide insertion.
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
            cursor: contents.len(),
            filepath: path,
            keydir: HashMap::new(),
        })
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
            .open(self.filepath.clone())?;

        Ok(())
    }

    pub fn put<'a>(&mut self, key: &'a str, value: &'a str) -> Result<()> {
        let pair = serde_json::to_string(&Pair {
            key,
            keysize: key.len(),
            value,
            value_size: value.len(),
            timestamp: SystemTime::now(),
        })?;

        let cursor = self.file.stream_position()?;
        let mut pos = std::cmp::max(cursor, self.cursor as u64);

        // check if file is larger than 5mb
        if pos >= 5120 - pair.len() as u64 {
            self.dropcurrentfile()?;
            pos = 0;
        };
        let _ = self.file.write(pair.as_bytes())?;
        let kdirval = KeydirVal {
            entry_size: pair.len(),
            entry_pos: pos,
            file_id: self.filepath.clone(),
            timestamp: SystemTime::now(),
        };
        self.keydir.insert(key.to_owned(), kdirval);
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
