use serde::{Serialize, de::DeserializeOwned};
use std::io::Seek;
use std::{
    collections::BTreeMap,
    env::{self},
    fs::{self, File, OpenOptions, ReadDir},
    io::{Read, Write, stdin},
    ops::{Add, DerefMut},
    os::windows::fs::FileExt,
    sync::{Arc, Mutex, mpsc},
    thread::{self},
    vec,
};

// Keep short for testing
const MAX_MEMTABLE: usize = 1 << 2;
const FOOTER_SIZE: usize = 1 << 4;

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
    let seq_bg = seq_num.clone();

    let _ = thread::spawn(move || {
        for flush_table in rx {
            let mut seq_num = seq_bg.lock().unwrap();
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
                let offset = buf.len() as u64; // could be u32

                let (key_len, value_len): (u32, u32) =
                    (k.as_bytes().len() as u32, v.as_bytes().len() as u32);
                // zero-bytes to indicate a key len in the byte stream?
                buf.extend_from_slice(&key_len.to_be_bytes());
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(&value_len.to_be_bytes());
                buf.extend_from_slice(v.as_bytes());

                if i % (flush_table.len() / 2) == 0 {
                    // Sparse index
                    println!("Put into index: {offset}");
                    idx.extend_from_slice(&key_len.to_be_bytes());
                    idx.extend_from_slice(k.as_bytes());
                    idx.extend_from_slice(&offset.to_be_bytes());
                }
            }
            let mut footer: Vec<u8> = vec![0; FOOTER_SIZE];
            // offset
            footer[0..FOOTER_SIZE / 2].copy_from_slice(&buf.len().to_be_bytes());
            // size
            footer[FOOTER_SIZE / 2..FOOTER_SIZE].copy_from_slice(&idx.len().to_be_bytes());

            buf.extend(&idx);
            buf.extend_from_slice(&footer);

            // println!("Idx len: {}, footer len: {}", idx.len(), footer.len());

            seg_handle
                .write_all(buf.as_slice())
                .expect("Unable to write");
            *seq_num = seq_num.add(1);

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
                    let curr_seq = seq_num.lock().unwrap().clone() - 1;
                    // while found_value.is_none() {
                    // Load the corresponding segment file
                    let mut seg_file = File::open(
                        curr_dir
                            .join(format!("segment_{}.sstable", curr_seq))
                            .as_path(),
                    )
                    .unwrap();
                    // Read the footer
                    let seg_size = seg_file.metadata().unwrap().len();

                    let mut footer_bytes: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
                    let footer_offset = (seg_size as usize - FOOTER_SIZE) as u64;
                    seg_file
                        .seek_read(&mut footer_bytes[0..FOOTER_SIZE], footer_offset)
                        .unwrap();

                    let idx_offset =
                        u64::from_be_bytes(footer_bytes[0..FOOTER_SIZE / 2].try_into().unwrap());
                    let idx_size = u64::from_be_bytes(
                        footer_bytes[FOOTER_SIZE / 2..FOOTER_SIZE]
                            .try_into()
                            .unwrap(),
                    );

                    let mut idx: Vec<u8> = vec![0; idx_size.try_into().unwrap()];
                    // Parse the sparse index (offset back from FOOTER_SIZE)
                    let n = seg_file.seek_read(&mut idx, idx_offset).unwrap();
                    assert_eq!(idx_size, n.try_into().unwrap());

                    let mut sparse_idx: BTreeMap<String, u64> = BTreeMap::new();
                    let mut idx_cursor = &idx[..];

                    while !idx_cursor.is_empty() {
                        // First key-offset
                        // key_len is u32 (4)
                        // key is a variable length byte slice
                        // offset is u64 (8)
                        let mut key_len_bytes: [u8; 4] = [0; 4];
                        idx_cursor.read_exact(&mut key_len_bytes).unwrap();
                        let key_len = u32::from_be_bytes(key_len_bytes);
                        let mut key_bytes = vec![0; key_len as usize];
                        idx_cursor.read_exact(&mut key_bytes).unwrap();
                        let mut offset_bytes: [u8; 8] = [0; 8];
                        idx_cursor.read_exact(&mut offset_bytes).unwrap();
                        let offset = u64::from_be_bytes(offset_bytes);

                        sparse_idx.insert(String::from_utf8(key_bytes).unwrap().into(), offset);
                    }

                    let mut key_offset: u64 = 0;
                    for (k, v) in sparse_idx.iter() {
                        match k.as_str().cmp(cmd_seq[1]) {
                            std::cmp::Ordering::Greater => break,
                            std::cmp::Ordering::Equal => {
                                key_offset = *v;
                                break;
                            }
                            std::cmp::Ordering::Less => {
                                key_offset = *v;
                            }
                        }
                    }
                    // Read until the end of the data block (either a zero byte or FOOTER_OFFET begins?)
                    // Assume for now that the key exists actually!
                    seg_file.seek(std::io::SeekFrom::Start(key_offset)).unwrap();

                    loop {
                        let current_position = seg_file.stream_position().unwrap();
                        if current_position >= idx_offset {
                            println!("Key not found");
                            break;
                        }

                        let mut key_len_bytes: [u8; 4] = [0; 4];
                        seg_file.read_exact(&mut key_len_bytes).unwrap();
                        let key_len = u32::from_be_bytes(key_len_bytes);

                        let mut key_bytes = vec![0; key_len as usize];
                        seg_file.read_exact(&mut key_bytes).unwrap();

                        let mut value_len_bytes: [u8; 4] = [0; 4];
                        seg_file.read_exact(&mut value_len_bytes).unwrap();

                        let value_len = u32::from_be_bytes(value_len_bytes);
                        let mut value_bytes = vec![0; value_len as usize];
                        seg_file.read_exact(&mut value_bytes).unwrap();

                        // Assuming just String keys and values for now!
                        let key = String::from_utf8(key_bytes).unwrap();
                        let value = String::from_utf8(value_bytes).unwrap();

                        if key.as_str() == cmd_seq[1] {
                            println!("Found pair: {key}: {value}");
                            break;
                        }
                        if key.as_str() > cmd_seq[1] {
                            println!("Key not found");
                            break;
                        }
                    }
                    // If not, decrement the curr_seq and try again (assuming no "missing" segments)
                    // This can be part of a higher order loop -> determine once you clean up?
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
