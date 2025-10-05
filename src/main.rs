use anyhow::Result;
use std::io::Seek;
use std::path::PathBuf;
use std::str::FromStr;
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

const MAX_MEMTABLE: usize = 1 << 2;
const FOOTER_SIZE: usize = 1 << 4;

struct KvStore<V> {
    memtable: BTreeMap<String, V>,
}

impl<V> KvStore<V>
where
    V: FromStr, // Serialize + Deserialize
{
    fn new() -> Self {
        Self {
            memtable: BTreeMap::new(),
        }
    }

    fn sync_wal(&mut self, mut file: File) -> Result<()>
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
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

struct SegmentIter {
    curr: usize,
    seg_ids: Vec<usize>,
    origin: PathBuf,
}

impl SegmentIter {
    fn new(seg_ids: Vec<usize>, origin_path: PathBuf) -> Self {
        Self {
            curr: seg_ids.len(),
            seg_ids: seg_ids,
            origin: origin_path,
        }
    }

    fn find_key_in_segments(self, key: &str) -> Result<Option<String>> {
        for mut seg in self {
            let mut key_offset: u64 = 0;
            for (k, v) in seg.idx.iter() {
                match k.as_str().cmp(key) {
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
            if let Ok(Some(v)) = seg.search(key, key_offset) {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

impl Iterator for SegmentIter {
    type Item = SegmentFile<String, u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.curr.checked_sub(1) {
            self.curr = curr;
        } else {
            return None;
        }

        let seg_path = self
            .origin
            .join(format!("segment_{}.sstable", self.seg_ids[self.curr]));

        if let Ok(mut seg_file) = File::open(seg_path) {
            let (offset, size) = SegmentFile::parse_footer(&mut seg_file).unwrap();
            let mut idx: Vec<u8> = vec![0; size.try_into().unwrap()];

            let n = seg_file.seek_read(&mut idx, offset).unwrap();
            assert_eq!(size, n.try_into().unwrap());

            let mut seg = SegmentFile::new(offset, seg_file);
            seg.populate_index(idx).unwrap();

            return Some(seg);
        }
        None
    }
}

struct SegmentFile<K, V> {
    seg_handle: File,
    idx_offset: u64,
    idx: BTreeMap<K, V>,
}

impl SegmentFile<String, u64> {
    fn new(offset: u64, seg_file: File) -> Self {
        Self {
            idx: BTreeMap::new(),
            idx_offset: offset,
            seg_handle: seg_file,
        }
    }

    fn search(&mut self, k: &str, key_offset: u64) -> Result<Option<String>> {
        self.seg_handle.seek(std::io::SeekFrom::Start(key_offset))?;
        loop {
            if self.seg_handle.stream_position()? >= self.idx_offset {
                return Ok(None);
            }

            let mut key_len_bytes: [u8; 4] = [0; 4];
            self.seg_handle.read_exact(&mut key_len_bytes)?;

            let key_len = u32::from_be_bytes(key_len_bytes);
            let mut key_bytes = vec![0; key_len as usize];
            self.seg_handle.read_exact(&mut key_bytes)?;

            let mut value_len_bytes: [u8; 4] = [0; 4];
            self.seg_handle.read_exact(&mut value_len_bytes)?;

            let value_len = u32::from_be_bytes(value_len_bytes);
            let mut value_bytes = vec![0; value_len as usize];
            self.seg_handle.read_exact(&mut value_bytes)?;

            let key = String::from_utf8(key_bytes)?;
            let value = String::from_utf8(value_bytes)?;

            if key.as_str() == k {
                return Ok(Some(value));
            }
            if key.as_str() > k {
                return Ok(None);
            }
        }
    }

    fn populate_index(&mut self, idx_bytes: Vec<u8>) -> Result<()> {
        let mut idx_cursor = &idx_bytes[..];
        while !idx_cursor.is_empty() {
            let mut key_len_bytes: [u8; 4] = [0; 4];
            idx_cursor.read_exact(&mut key_len_bytes)?;

            let key_len = u32::from_be_bytes(key_len_bytes);
            let mut key_bytes = vec![0; key_len as usize];
            idx_cursor.read_exact(&mut key_bytes)?;

            let mut offset_bytes: [u8; 8] = [0; 8];
            idx_cursor.read_exact(&mut offset_bytes)?;

            let offset = u64::from_be_bytes(offset_bytes);
            self.idx
                .insert(String::from_utf8(key_bytes)?.into(), offset);
        }
        Ok(())
    }

    fn parse_footer(seg_file: &mut File) -> Result<(u64, u64)> {
        let seg_size = seg_file.metadata()?.len();

        let mut footer_bytes: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        let footer_offset = (seg_size as usize - FOOTER_SIZE) as u64;
        seg_file.seek_read(&mut footer_bytes[0..FOOTER_SIZE], footer_offset)?;

        let idx_offset = u64::from_be_bytes(footer_bytes[0..FOOTER_SIZE / 2].try_into()?);
        let idx_size = u64::from_be_bytes(footer_bytes[FOOTER_SIZE / 2..FOOTER_SIZE].try_into()?);

        Ok((idx_offset, idx_size))
    }
}

#[allow(unused_assignments)]
fn main() {
    let curr_dir = env::current_dir().expect("curr dir");
    // Serializable K, V
    let mut hm: KvStore<String> = KvStore::new();
    let (tx, rx) = mpsc::channel::<BTreeMap<String, String>>();

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
            .append(true) // Append-only
            .create(true)
            .open(curr_dir.join(format!("wal.log")).as_path())
            .expect("New log file error"),
    ));

    let curr_dir_bg = curr_dir.clone();
    let log_handle_bg = log_handle.clone();
    let seg_bg = seg_num.clone();

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
            footer[0..FOOTER_SIZE / 2].copy_from_slice(&buf.len().to_be_bytes());
            footer[FOOTER_SIZE / 2..FOOTER_SIZE].copy_from_slice(&idx.len().to_be_bytes());

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

            // Merge segments...
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
                    let curr_seg = seg_num.lock().unwrap().clone();
                    let seg_iter =
                        SegmentIter::new((0..curr_seg).into_iter().collect(), curr_dir.clone());
                    if let Ok(Some(value)) = seg_iter.find_key_in_segments(cmd_seq[1]) {
                        println!("GET -> {}", value)
                    } else {
                        println!("Not found")
                    }
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
