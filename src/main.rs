use std::{collections::BTreeMap, env, fs::OpenOptions, io::Write};

// How to make value be anything -> trait use?

#[allow(unused_assignments)]
fn main() {
    // Keys / values as strings
    // In-memory sotore (memtable) for access
    let mut hm = BTreeMap::new();
    let mut seq_num = 0;

    let mut log_handle = OpenOptions::new()
        .read(true)
        .write(true)
        .append(true) // Append-only
        .create(true)
        .open(
            env::current_dir()
                .expect("Cwd not found")
                .join("log.txt")
                .as_path(),
        )
        .expect("New log file error");

    for i in 0..10 {
        // Setting a key -> memtable and WAL (first)!
        let (k, v) = (format!("key_{i}"), format!("value_{i}"));

        // How many bytes long the value / key is?
        let (key_len, value_len) = (k.len(), v.len());

        assert!(key_len <= u8::MAX as usize);
        assert!(value_len <= u8::MAX as usize);

        let log_entry = format!("SET {}, {}, {}, {}\n", k, key_len, v, value_len);
        log_handle
            .write_all(log_entry.as_bytes())
            .expect("Could not write contents");

        hm.insert(k.clone(), v.clone());
    }

    // Flush to a new SSTable segment
    // [key_len][key...][value_len][value...]...

    // new segment .sstable
    let mut seg_handle = OpenOptions::new()
        .write(true)
        .create(true)
        .open(
            env::current_dir()
                .expect("Cwd not found")
                .join(format!("segment_{seq_num}.sstable"))
                .as_path(),
        )
        .expect("New log file error");
    seq_num += 1;

    // Flush logic
    let mut buf = Vec::new();
    for (k, v) in hm.iter() {
        let (key_len, value_len) = (k.len(), v.len());
        buf.push(key_len as u8);
        buf.extend_from_slice(k.as_bytes());
        buf.push(value_len as u8);
        buf.extend_from_slice(v.as_bytes());
    }
    seg_handle
        .write_all(buf.as_slice())
        .expect("Unable to write");
    hm.clear();

    // Merge segments
    // Seek values from segments
    // ...
}
