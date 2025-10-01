use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    env::{self},
    fs::{self, File, OpenOptions, ReadDir},
    io::{Read, Write, stdin},
    ops::DerefMut,
    sync::{Arc, Mutex, mpsc},
    thread::{self},
};

// Keep short for testing
const MAX_MEMTABLE: usize = 1 << 2;
const FOOTER_SIZE: usize = 1 << 2;

struct KvStore<K, V> {
    memtable: BTreeMap<K, V>,
}

impl<K, V> KvStore<K, V>
where
    K: Ord + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn new() -> Self {
        Self {
            memtable: BTreeMap::new(),
        }
    }
}

#[allow(unused_assignments)]
fn main() {
    let curr_dir = env::current_dir().expect("curr dir");

    // Serializable K, V
    let mut hm: KvStore<String, String> = KvStore::new();
    let (tx, rx) = mpsc::channel::<BTreeMap<String, String>>();

    if let Ok(mut file) = File::open(curr_dir.join("wal.log")) {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();

        for entry in buf.lines() {
            let cmd_seq: Vec<_> = entry.split_whitespace().collect();
            match cmd_seq[0] {
                "SET" => {
                    // Assumes right format
                    hm.memtable
                        .insert(cmd_seq[1].to_string(), cmd_seq[2].to_string());
                }
                _ => {}
            }
        }
    }

    let latest_segment = get_dir_segment_count(
        fs::read_dir(env::current_dir().expect("curr dir")).expect("show dir"),
    );

    // Does this need to be behind a mutex?
    let seq_num: Arc<Mutex<usize>> = Arc::new(Mutex::new(latest_segment));
    let log_handle = Arc::new(Mutex::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .append(true) // Append-only
            .create(true)
            .open(curr_dir.join(format!("wal.log")).as_path())
            .expect("New log file error"),
    ));

    let curr_dir_bg = curr_dir.clone();
    let log_handle_bg = log_handle.clone();
    let _ = thread::spawn(move || {
        for flush_table in rx {
            let mut seq_num = seq_num.lock().unwrap().to_owned();
            let mut seg_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .open(
                    curr_dir_bg
                        .join(format!("segment_{}.sstable", seq_num))
                        .as_path(),
                )
                .expect("New log file error");

            // Flush -> bytes vs. other encoding?
            let mut idx = Vec::new();
            let mut buf = Vec::new();

            for (i, (k, v)) in flush_table.iter().enumerate() {
                let (key_len, value_len) = (k.len(), v.len());
                buf.extend_from_slice(&key_len.to_be_bytes());
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(&value_len.to_be_bytes());
                buf.extend_from_slice(v.as_bytes());

                if i % (flush_table.len() / 4) == 0 {
                    // Sparse index
                    idx.extend_from_slice(&key_len.to_be_bytes());
                    idx.extend_from_slice(k.as_bytes());
                    idx.extend_from_slice(&value_len.to_be_bytes());
                    idx.extend_from_slice(v.as_bytes());
                    idx.extend_from_slice(&i.to_be_bytes());
                }
            }
            let mut footer: Vec<u8> = vec![0; 4];
            footer[0..4].copy_from_slice(&idx.len().to_be_bytes());
            
            buf.extend(&idx);
            buf.extend_from_slice(&footer);

            seg_handle
                .write_all(buf.as_slice())
                .expect("Unable to write");
            seq_num += 1;

            let mut log = log_handle_bg.lock().unwrap();

            fs::remove_file(curr_dir_bg.join(format!("wal.log")).as_path()).unwrap();

            let _ = std::mem::replace(
                log.deref_mut(),
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(curr_dir_bg.join(format!("wal.log")).as_path())
                    .expect("New log file error"),
            );

            // Merge segments
        }
    });

    let lines = stdin().lines();
    for line in lines {
        let line = line.unwrap();
        let cmd_seq: Vec<_> = line.split_whitespace().collect();

        match cmd_seq[0] {
            "SET" => {
                assert!(cmd_seq[1..].len() == 2);
                let (k, v) = (cmd_seq[1], cmd_seq[2]);
                let (key_len, value_len) = (k.len(), v.len());

                // Lengths as bytes -> reading?

                assert!(key_len <= u8::MAX as usize);
                assert!(value_len <= u8::MAX as usize);

                let log_entry = format!("SET {} {}\n", k, v);
                log_handle
                    .lock()
                    .unwrap()
                    .write_all(log_entry.as_bytes())
                    .expect("Could not write contents");

                hm.memtable.insert(k.to_string(), v.to_string());

                let tx = tx.clone();

                if hm.memtable.len() >= MAX_MEMTABLE {
                    let flush_table = std::mem::take(&mut hm.memtable);
                    tx.send(flush_table).unwrap();
                }

                println!("SET done")
            }
            "GET" => {
                assert!(cmd_seq[1..].len() == 1);

                if let Some(val) = hm.memtable.get(cmd_seq[1]) {
                    println!("GET -> {}", val.clone())
                } else {
                    // Assume no merging / compaction in the beginning!
                    todo!()
                }
            }
            _ => {
                panic!("Unknown command")
            }
        }
    }
}

fn get_dir_segment_count(dir: ReadDir) -> usize {
    dir.filter(|path_result| {
        path_result
            .as_ref()
            .unwrap()
            .file_name()
            .as_os_str()
            .to_str()
            .unwrap()
            .starts_with("segment")
    })
    .count()
}
