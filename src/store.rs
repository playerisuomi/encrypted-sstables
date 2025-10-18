use crate::FOOTER_SIZE;
use crate::MAX_MEMTABLE;
use crate::encryption::DecryptError;
use crate::encryption::Decrypter;
use crate::encryption::DefaultDecrypter;
use crate::encryption::DefaultEncrypter;
use crate::encryption::EncryptError;
use crate::encryption::Encrypter;
use crate::segment::SegmentIter;
use anyhow::Result;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use core::fmt;
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::io::stdin;
use std::ops::Add;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    fs::File,
    str::FromStr,
};

#[derive(Debug)]
pub struct KvStore<V> {
    pub memtable: Arc<Mutex<BTreeMap<String, V>>>,
    encypter_guard: Arc<Mutex<DefaultEncrypter>>,
    seq_num: Arc<Mutex<usize>>,
    log_handle: Arc<Mutex<File>>,

    password: String,
    curr_dir: PathBuf,

    flush_tx: Sender<BTreeMap<String, V>>,
    flush_rx: Arc<Mutex<Receiver<BTreeMap<String, V>>>>,
}

impl<V> KvStore<V>
where
    V: bincode::Decode<()> + FromStr + bincode::Encode + Send + Sync + Display,
    <V as FromStr>::Err: Debug,
{
    pub fn new(password: String, latest_segment: usize, curr_dir: PathBuf) -> Self {
        let encrypter = DefaultEncrypter::new(password.to_owned()).expect("no default encryption");
        let (tx, rx) = mpsc::channel::<BTreeMap<String, V>>();
        Self {
            log_handle: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(curr_dir.join(format!("wal.log")).as_path())
                    .expect("New log file error"),
            )),
            seq_num: Arc::new(Mutex::new(latest_segment)),
            memtable: Arc::new(Mutex::new(BTreeMap::new())),
            flush_rx: Arc::new(Mutex::new(rx)),
            encypter_guard: Arc::new(Mutex::new(encrypter)),
            password: password,
            curr_dir: curr_dir,
            flush_tx: tx,
        }
    }

    pub fn run(&self) -> Result<(), KvError> {
        if let Ok(file) = File::open(self.curr_dir.join("wal.log")) {
            self.sync_wal(file)?
        }

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

                    self.write_wal(k.to_string(), v.parse().expect("invalid value"))
                        .expect("write wal");
                    self.insert_parsed(k.to_string(), v.to_string())?;

                    let tx: mpsc::Sender<BTreeMap<String, V>> = self.flush_tx.clone();
                    let mut memtable = self.memtable.lock().expect("get mut");
                    if memtable.len() >= MAX_MEMTABLE {
                        let flush_table: BTreeMap<String, _> = std::mem::take(&mut memtable);
                        tx.send(flush_table).unwrap();
                    }

                    println!("SET done")
                }
                "GET" => {
                    assert!(cmd_seq[1..].len() == 1);
                    if let Some(val) = self.memtable.lock().expect("get lock").get(cmd_seq[1]) {
                        println!("GET -> {}", val)
                    } else {
                        let curr_seg = self.seq_num.lock().unwrap().clone();

                        let seg_iter = SegmentIter::new(
                            (0..curr_seg).into_iter().collect(),
                            self.curr_dir.clone(),
                            self.password.to_owned(),
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

    pub fn run_bg_thread(&self) {
        let log_handle_bg = self.log_handle.clone();
        let seq_bg = self.seq_num.clone();
        let encrypter_bg = self.encypter_guard.clone();

        for flush_table in self.flush_rx.lock().expect("rx lock").iter() {
            let mut seq_num = seq_bg.lock().unwrap();
            let mut seq_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .open(
                    self.curr_dir
                        .join(format!("segment_{}.sstable", seq_num))
                        .as_path(),
                )
                .expect("New log file error");

            let mut idx = Vec::new();
            let mut buf = Vec::new();

            for (i, (k, v)) in flush_table.iter().enumerate() {
                let offset = buf.len() as u64;
                let (sealed_bytes, nonce) =
                    self.build_entry(&k, &v).expect("unable to build entry");

                let key_len: u32 = k.as_bytes().len() as u32;
                let cipher_len: u32 = sealed_bytes.len() as u32;

                buf.extend_from_slice(&key_len.to_be_bytes());
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(&nonce);
                buf.extend_from_slice(&cipher_len.to_be_bytes());
                buf.extend_from_slice(sealed_bytes.as_slice());

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
            encrypter_bg
                .lock()
                .expect("encrypter lock")
                .decode_salt_bytes(&mut salt_bytes)
                .expect("salt decode");

            footer[16..].copy_from_slice(&salt_bytes);

            buf.extend(&idx);
            buf.extend_from_slice(&footer);

            seq_handle
                .write_all(buf.as_slice())
                .expect("unable to write");
            *seq_num = seq_num.add(1);

            rotate_log_file(
                &&log_handle_bg,
                &self.curr_dir.join("wal.log").as_path(),
                &self.curr_dir.join("archive").as_path(),
            )
            .expect("rotate log")

            // Note: Merge segments...
        }
    }

    pub fn sync_wal(&self, mut file: File) -> Result<(), KvError> {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();

        for entry in buf.lines() {
            let cmd_seq: Vec<_> = entry.split_whitespace().collect();
            match cmd_seq[0] {
                "SET" => {
                    assert!(cmd_seq.len() == 5);
                    let mut key: Vec<u8> = Vec::from(cmd_seq[1].as_bytes());
                    let mut enc_string: Vec<u8> =
                        BASE64_STANDARD.decode(cmd_seq[2]).expect("decode value");
                    let nonce: Vec<u8> = BASE64_STANDARD.decode(cmd_seq[3]).expect("decode nonce");
                    let salt_bytes: Vec<u8> =
                        BASE64_STANDARD.decode(cmd_seq[4]).expect("decode nonce");

                    let enc_bytes = enc_string.as_mut_slice();
                    let nonce_bytes: [u8; 12] = nonce.try_into().expect("invalid nonce bytes");
                    let salt = DefaultDecrypter::encode_salt_string(salt_bytes.as_slice())
                        .expect("salt string conversion");

                    let log_decrypter = DefaultDecrypter::new(self.password.clone(), salt);

                    if let Ok(plaintext_bytes) =
                        log_decrypter.decrypt(enc_bytes, nonce_bytes, &mut key)
                    {
                        let (plain, _) = bincode::decode_from_slice(
                            plaintext_bytes,
                            bincode::config::standard(),
                        )?;
                        self.memtable
                            .lock()
                            .expect("insert lock")
                            .insert(cmd_seq[1].to_string(), plain);
                    }
                }
                _ => return Err(KvError("unknown cmd")),
            };
        }
        Ok(())
    }

    pub fn write_wal(&self, k: String, v: V) -> Result<(), KvError>
    where
        V: bincode::Encode,
    {
        let (mut sealed_bytes, mut nonce) = self.build_entry(&k, &v)?;

        let encrypter = self.encypter_guard.lock().expect("enuable to aqcuire lock");
        let mut salt_bytes: [u8; 16] = [0u8; 16];
        encrypter
            .decode_salt_bytes(&mut salt_bytes)
            .expect("salt decode");

        let encoded_string = BASE64_STANDARD.encode(&mut sealed_bytes);
        let nonce = BASE64_STANDARD.encode(&mut nonce);
        let salt_encoded = BASE64_STANDARD.encode(&mut salt_bytes);

        let log_entry = format!("SET {} {} {} {}\n", k, encoded_string, nonce, salt_encoded);
        let mut buf = Vec::from(log_entry.as_bytes());

        self.log_handle
            .lock()
            .expect("unable to lock file")
            .write_all(&mut buf)?;
        Ok(())
    }

    pub fn insert_parsed(&self, key: String, value: String) -> Result<(), KvError> {
        if let Ok(value) = value.parse::<V>() {
            self.memtable
                .lock()
                .expect("insert lock")
                .insert(key, value);
        } else {
            return Err(KvError("insert"));
        }
        Ok(())
    }

    fn build_entry(&self, key: &str, value: &V) -> Result<(Vec<u8>, [u8; 12]), KvError> {
        let encrypter = self
            .encypter_guard
            .lock()
            .expect("unable to acquire a lock");
        let mut sealed_bytes = bincode::encode_to_vec(value, bincode::config::standard())?;
        let nonce = encrypter.encrypt(&mut sealed_bytes, Some(key.as_bytes()))?;
        Ok((sealed_bytes, nonce))
    }
}

fn rotate_log_file(
    log_handle: &Arc<Mutex<File>>,
    log_path: &Path,
    archive_dir: &Path,
) -> Result<()> {
    fs::create_dir_all(archive_dir)?;

    let mut log_guard = log_handle.lock().unwrap();
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

#[derive(Debug, Clone)]
pub struct KvError(pub &'static str);

impl Display for KvError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "KvStore: Context '{}'", self.0)
    }
}

// For now
impl From<std::io::Error> for KvError {
    fn from(_: std::io::Error) -> Self {
        KvError("IO error")
    }
}

impl From<EncryptError> for KvError {
    fn from(_value: EncryptError) -> Self {
        KvError("Failed to encrypt")
    }
}

impl From<DecryptError> for KvError {
    fn from(_value: DecryptError) -> Self {
        KvError("Failed to decrypt")
    }
}

impl From<bincode::error::EncodeError> for KvError {
    fn from(_: bincode::error::EncodeError) -> Self {
        KvError("Failed to encode")
    }
}

impl From<bincode::error::DecodeError> for KvError {
    fn from(_: bincode::error::DecodeError) -> Self {
        KvError("Failed to decode")
    }
}
