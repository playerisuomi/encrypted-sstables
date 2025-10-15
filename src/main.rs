use anyhow::Result;
use argon2::Argon2;
use argon2::PasswordHasher;
use argon2::password_hash::{SaltString, rand_core::OsRng};
use enc_kv_store::NoncePlaceholder;
use enc_kv_store::segment::SegmentIter;
use enc_kv_store::store::{KvError, KvStore};
use enc_kv_store::{FOOTER_SIZE, MAX_MEMTABLE};
use ring::aead;
use ring::aead::BoundKey;
use std::sync::LazyLock;
use std::{
    collections::BTreeMap,
    env::{self},
    fs::{self, File, OpenOptions, ReadDir},
    io::{Write, stdin},
    ops::{Add, DerefMut},
    sync::{Arc, Mutex, mpsc},
    thread::{self},
    vec,
};

static SALT: LazyLock<SaltString> = std::sync::LazyLock::new(|| SaltString::generate(&mut OsRng));

#[allow(unused_assignments)]
fn main() -> Result<(), KvError> {
    let args: Vec<String> = env::args().collect();
    assert!(args.len() <= 2);

    // Handle!
    let password = args.get(1).unwrap();
    println!("{password}");

    let curr_dir = env::current_dir().expect("curr dir");

    // TODO: KvStore to take an encrypter as a field?
    // KvStore to hold the key!
    let mut hm: KvStore<String> = KvStore::new();
    let (tx, rx) = mpsc::channel::<BTreeMap<String, String>>();

    // Note: Handle errors
    let argon2 = Argon2::default();
    // Store in a Mutex?
    let key = argon2
        .hash_password(password.as_bytes(), SALT.as_salt())
        .unwrap();
    let key_mut = Arc::new(Mutex::new(key));

    if let Ok(file) = File::open(curr_dir.join("wal.log")) {
        hm.sync_wal(file).unwrap();
    }
    let latest_segment = get_dir_segment_count(
        fs::read_dir(env::current_dir().expect("curr dir")).expect("show dir"),
    );

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
    let key_bg = key_mut.clone();

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

            for (i, (k, v)) in flush_table.iter().enumerate() {
                let offset = buf.len() as u64; // could be u32

                // TODO: Serde serialization -> derive Serialize and DeserializeOwned
                // More generic method to serialize the value! -> might not be a String

                let nonce = NoncePlaceholder::new();

                // Handle! -> separate later
                let key_mut = key_bg.lock().unwrap().hash.unwrap();
                let key_bytes = key_mut.as_bytes();

                let u_key = aead::UnboundKey::new(&aead::AES_256_GCM, key_bytes).unwrap();
                let mut sealing_key = aead::SealingKey::new(u_key, nonce.clone());

                let mut sealed_value: Vec<u8> = Vec::from(v.as_bytes());
                sealing_key
                    .seal_in_place_append_tag(aead::Aad::from(k.as_bytes()), &mut sealed_value)
                    .expect("sealing");

                let key_len: u32 = k.as_bytes().len() as u32;
                let cipher_len: u32 = sealed_value.len() as u32;

                buf.extend_from_slice(&key_len.to_be_bytes());
                buf.extend_from_slice(k.as_bytes());

                // Nonce (12)
                buf.extend_from_slice(&nonce.n);
                // Cipher (..)
                buf.extend_from_slice(&cipher_len.to_be_bytes());
                buf.extend_from_slice(sealed_value.as_slice());

                if i % (flush_table.len() / 2) == 0 {
                    // Sparse index
                    println!("Put into index: {offset}");
                    idx.extend_from_slice(&key_len.to_be_bytes());
                    idx.extend_from_slice(k.as_bytes());
                    idx.extend_from_slice(&offset.to_be_bytes());
                }
            }
            // Key hash into the footer as plaintext
            // len from 10 bytes to 64 bytes -> reserve 64 for now?

            let mut footer: Vec<u8> = vec![0; FOOTER_SIZE];
            // usize -> 8 bytes
            footer[0..8].copy_from_slice(&buf.len().to_be_bytes());
            footer[8..16].copy_from_slice(&idx.len().to_be_bytes());

            // Salt length is 16 bytes ????
            let mut salt_bytes: [u8; 16] = [0u8; 16];
            SALT.decode_b64(&mut salt_bytes)
                .expect("salt decoding into bytes");
            footer[16..].copy_from_slice(&salt_bytes);

            buf.extend(&idx);
            buf.extend_from_slice(&footer);

            seg_handle
                .write_all(buf.as_slice())
                .expect("Unable to write");
            *seg_num = seg_num.add(1);

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

                // TODO: encryption here!
                let log_entry = format!("SET {} {}\n", k, v);
                log_handle
                    .lock()
                    .unwrap()
                    .write_all(log_entry.as_bytes())
                    .expect("Could not write contents");

                // Parsed into V (FromStr)
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
                    // TODO: a decrypter struct for SegmentIter -> centralized decrpytion logic?
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

fn get_dir_segment_count(dir: ReadDir) -> usize {
    dir.filter(|path_result| {
        path_result
            .as_ref()
            .unwrap()
            .file_name()
            .as_os_str()
            .to_str()
            .unwrap()
            .starts_with("segment_")
    })
    .count()
}

#[cfg(test)]
mod tests {
    use enc_kv_store::store::KvStore;
    use rand::Rng;
    use rand::distr::{Alphanumeric, Uniform};

    #[test]
    fn test_random_str_type_once() {
        let mut hm = KvStore::new();
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

    // Concurrency, on-disk saving, etc. to find edge cases?
    // WALs
}
