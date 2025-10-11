use core::fmt;
use std::io::Read;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    fs::File,
    str::FromStr,
};

#[derive(Debug, Clone)]
pub struct KvError(pub &'static str);

impl Display for KvError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "KvStore: Context '{}'", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct KvStore<V> {
    pub memtable: BTreeMap<String, V>,
}

impl<V> KvStore<V>
where
    V: FromStr, // Serialize + Deserialize
{
    pub fn new() -> Self {
        Self {
            memtable: BTreeMap::new(),
        }
    }

    pub fn sync_wal(&mut self, mut file: File) -> Result<(), KvError>
    where
        V: FromStr,
        <V as FromStr>::Err: std::fmt::Debug,
    {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();

        for entry in buf.lines() {
            let cmd_seq: Vec<_> = entry.split_whitespace().collect();
            match cmd_seq[0] {
                "SET" => {
                    if let Ok(value) = cmd_seq[2].parse::<V>() {
                        self.memtable.insert(cmd_seq[1].to_string(), value);
                    } else {
                        return Err(KvError("sync wal"));
                    }
                }
                _ => return Err(KvError("unknown cmd")),
            };
        }
        Ok(())
    }
}
