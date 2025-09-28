use std::{
    collections::BTreeMap,
    env,
    fs::{self, OpenOptions},
    io::{Write, stdin},
    sync::{Arc, Mutex, mpsc},
    thread::{self},
};

const MAX_MEMTABLE: usize = 1 << 2;

// How to make value be anything -> trait use?

#[allow(unused_assignments)]
fn main() {
    let mut hm: BTreeMap<String, String> = BTreeMap::new();
    let (tx, rx) = mpsc::channel::<BTreeMap<String, String>>();

    let mut log_dir = fs::read_dir(env::current_dir().expect("curr dir")).expect("show dir");
    if log_dir.any(|path_result| path_result.unwrap().file_name() == "wal.log") {
        // Load into the memtable
        println!("Found an uncommited log")
    }

    // TODO -> find a more efficient way (manifest file, etc)
    // For now, get the seq num from the "latest" segment found
    let latest_segment = fs::read_dir(env::current_dir().expect("curr dir"))
        .expect("show dir")
        .filter(|path_result| {
            path_result
                .as_ref()
                .unwrap()
                .file_name()
                .as_os_str()
                .to_str()
                .unwrap()
                .starts_with("segment")
        })
        .count();

    let seq_num: Arc<Mutex<usize>> = Arc::new(Mutex::new(latest_segment));

    // New log per new memtable -> TODO: into Arc<Mutex<>>
    let mut log_handle = OpenOptions::new()
        .read(true)
        .write(true)
        .append(true) // Append-only
        .create(true)
        .open(
            env::current_dir()
                .expect("Cwd not found")
                .join(format!("wal.log"))
                .as_path(),
        )
        .expect("New log file error");

    // SPAWN the background thread -> after initializing the data structures and loading them into memory
    // Run until receives an exit signal!

    let _ = thread::spawn(move || {
        for flush_table in rx {
            // Flush to a new SSTable segment
            // [key_len][key...][value_len][value...]...
            let mut seq_num = seq_num.lock().unwrap().to_owned();
            let mut seg_handle = OpenOptions::new()
                .write(true)
                .create(true)
                .open(
                    env::current_dir()
                        .expect("Cwd not found")
                        .join(format!("segment_{}.sstable", seq_num))
                        .as_path(),
                )
                .expect("New log file error");

            // Flush logic -> bytes vs. another encoding
            let mut buf = Vec::new();
            for (k, v) in flush_table.iter() {
                let (key_len, value_len) = (k.len(), v.len());
                buf.push(key_len as u8);
                buf.extend_from_slice(k.as_bytes());
                buf.push(value_len as u8);
                buf.extend_from_slice(v.as_bytes());
            }

            seg_handle
                .write_all(buf.as_slice())
                .expect("Unable to write");
            seq_num += 1; // Write to SEQ -> now it's safe to disregard older WALs

            // Merge segments
            // ...
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

                let log_entry = format!("SET {}, {}\n", k, v);
                log_handle
                    .write_all(log_entry.as_bytes())
                    .expect("Could not write contents");

                hm.insert(k.to_string(), v.to_string());

                let tx = tx.clone();

                if hm.len() >= MAX_MEMTABLE {
                    let flush_table = std::mem::replace(&mut hm, BTreeMap::new());
                    tx.send(flush_table).unwrap();
                }

                println!("SET done")
            }
            "GET" => {
                assert!(cmd_seq[1..].len() == 1);

                if let Some(val) = hm.get(cmd_seq[1]) {
                    println!("GET -> {}", val.clone())
                } else {
                    // TODO: load segments -> algo for this?
                    panic!("Not found in memory")
                }
            }
            _ => {
                panic!("Unknown command")
            }
        }
    }
}
