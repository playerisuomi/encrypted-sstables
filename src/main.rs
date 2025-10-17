use anyhow::Result;
use enc_kv_store::encryption::DefaultEncrypter;
use enc_kv_store::encryption::Encrypter;
use enc_kv_store::segment::SegmentIter;
use enc_kv_store::store::{KvError, KvStore};
use enc_kv_store::{FOOTER_SIZE, MAX_MEMTABLE};
use once_cell::sync::Lazy;
use std::path::Path;
use std::{
    collections::BTreeMap,
    env::{self},
    fs::{self, File, OpenOptions},
    io::{Write, stdin},
    ops::{Add, DerefMut},
    sync::{Arc, Mutex, mpsc},
    thread::{self},
    vec,
};

static DEFAULT: Lazy<String> = Lazy::new(|| String::from("password"));

#[allow(unused_assignments)]
fn main() -> Result<(), KvError> {
    let args: Vec<String> = env::args().collect();
    assert!(args.len() <= 2);

    // Config?
    let password = args.get(1).unwrap_or(&DEFAULT);
    let curr_dir = env::current_dir().expect("curr dir");

    let encrypter = DefaultEncrypter::new(password.to_owned()).expect("no default encryption");
    let mut hm: KvStore<String> = KvStore::new(encrypter.clone());
    let (tx, rx) = mpsc::channel::<BTreeMap<String, String>>();

    let encypter_guard = Arc::new(Mutex::new(encrypter));

    if let Ok(file) = File::open(curr_dir.join("wal.log")) {
        hm.sync_wal(file, password.clone()).unwrap();
    }
    let latest_segment = get_dir_segment_count(env::current_dir().expect("curr dir").as_path())
        .expect("segment count");

    let seg_num: Arc<Mutex<usize>> = Arc::new(Mutex::new(latest_segment));
    let log_handle = Arc::new(Mutex::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(curr_dir.join(format!("wal.log")).as_path())
            .expect("New log file error"),
    ));

    let curr_dir_bg = curr_dir.clone();
    let log_handle_bg = log_handle.clone();
    let seg_bg = seg_num.clone();
    let encrypter_bg = encypter_guard.clone();

    let _ = thread::spawn(move || {
        for flush_table in rx {
            let mut seg_num = seg_bg.lock().unwrap();
            let mut seg_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .open(
                    curr_dir_bg
                        .join(format!("segment_{}.sstable", seg_num))
                        .as_path(),
                )
                .expect("New log file error");

            let mut idx = Vec::new();
            let mut buf = Vec::new();

            let encrypter = encrypter_bg.lock().unwrap();

            for (i, (k, v)) in flush_table.iter().enumerate() {
                let offset = buf.len() as u64;
                // TODO: Serde serialization -> derive Serialize and DeserializeOwned
                // More generic method to serialize the value! -> might not be a String
                let mut sealed_value: Vec<u8> = Vec::from(v.as_bytes());
                let n = encrypter
                    .encrypt(&mut sealed_value, Some(k.as_bytes()))
                    .expect("encrypting a value-pair");

                let key_len: u32 = k.as_bytes().len() as u32;
                let cipher_len: u32 = sealed_value.len() as u32;

                buf.extend_from_slice(&key_len.to_be_bytes());
                buf.extend_from_slice(k.as_bytes());
                // Nonce (12)
                buf.extend_from_slice(&n);
                // Cipher (..)
                buf.extend_from_slice(&cipher_len.to_be_bytes());
                buf.extend_from_slice(sealed_value.as_slice());

                if i % (flush_table.len() / 2) == 0 {
                    idx.extend_from_slice(&key_len.to_be_bytes());
                    idx.extend_from_slice(k.as_bytes());
                    idx.extend_from_slice(&offset.to_be_bytes());
                }
            }
            let mut footer: Vec<u8> = vec![0; FOOTER_SIZE];
            footer[0..8].copy_from_slice(&buf.len().to_be_bytes());
            footer[8..16].copy_from_slice(&idx.len().to_be_bytes());

            let mut salt_bytes: [u8; 16] = [0u8; 16];
            encrypter
                .decode_salt_bytes(&mut salt_bytes)
                .expect("salt decode");

            footer[16..].copy_from_slice(&salt_bytes);

            buf.extend(&idx);
            buf.extend_from_slice(&footer);

            seg_handle
                .write_all(buf.as_slice())
                .expect("unable to write");
            *seg_num = seg_num.add(1);

            rotate_log_file(
                &&log_handle_bg,
                &curr_dir_bg.join("wal.log").as_path(),
                &curr_dir_bg.join("archive").as_path(),
            )
            .expect("rotate log")

            // Note: Merge segments...
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

                hm.write_wal(
                    k.to_string(),
                    v.to_string(),
                    log_handle.lock().unwrap().deref_mut(),
                )
                .expect("write wal");
                hm.insert_parsed(k.to_string(), v.to_string())?;

                let tx = tx.clone();
                if hm.memtable.len() >= MAX_MEMTABLE {
                    let flush_table: BTreeMap<String, _> = std::mem::take(&mut hm.memtable);
                    tx.send(flush_table).unwrap();
                }

                println!("SET done")
            }
            "GET" => {
                assert!(cmd_seq[1..].len() == 1);
                if let Some(val) = hm.memtable.get(cmd_seq[1]) {
                    println!("GET -> {}", val.clone())
                } else {
                    let curr_seg = seg_num.lock().unwrap().clone();

                    let seg_iter = SegmentIter::new(
                        (0..curr_seg).into_iter().collect(),
                        curr_dir.clone(),
                        password.to_owned(),
                    );
                    if let Ok(Some(value)) = seg_iter.find_key_in_segments(cmd_seq[1]) {
                        println!("GET -> {}", value)
                    } else {
                        println!("Not found")
                    }
                }
            }
            _ => return Err(KvError("Unknown cmd")),
        }
    }
    Ok(())
}

fn rotate_log_file(
    log_handle: &Arc<Mutex<File>>,
    log_path: &Path,
    archive_dir: &Path,
) -> Result<()> {
    let mut log_guard = log_handle.lock().unwrap();
    fs::create_dir_all(archive_dir)?;

    let archive_path = archive_dir.join("wal_log");
    if log_path.exists() {
        fs::rename(log_path, &archive_path)?;
    }

    let new_log_file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(log_path)?;

    *log_guard = new_log_file;
    Ok(())
}

fn get_dir_segment_count(dir_path: &Path) -> Result<usize> {
    Ok(fs::read_dir(dir_path)?
        .filter_map(|entry_result| {
            let entry = entry_result.ok()?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_str()?;
            if file_name_str.starts_with("segment_") {
                Some(())
            } else {
                None
            }
        })
        .count())
}

#[cfg(test)]
mod tests {
    use enc_kv_store::encryption::DefaultEncrypter;
    use enc_kv_store::store::KvStore;
    use rand::Rng;
    use rand::distr::{Alphanumeric, Uniform};

    #[test]
    fn test_random_str_type_once() {
        let mut hm = KvStore::new(DefaultEncrypter::new(String::new()).unwrap());
        // Custom function for "randoms" in KvStore?
        let (k, v): (String, usize) = (
            rand::rng()
                .sample_iter(&Alphanumeric)
                .take(1000)
                .map(char::from)
                .collect(),
            rand::rng().sample(Uniform::new(10usize, 15).unwrap()),
        );
        hm.memtable.insert(k.clone(), v);
        assert_eq!(&v, hm.memtable.get(&k).unwrap());
    }

    // #[test]
    // fn test_random_str_store_and_retrieve() {
    //     let hm: KvStore<String> = KvStore::new();
    // }

    // Concurrency, on-disk saving, etc. to find edge cases?
    // WALs
}
