# enc-kv-store

### Notes

- SSTables -> background thread(pool) to merge LSM-Trees
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)
  - Sleeps (_Condvar_) and listens for signals (_mpsc_)

### Next

- Merge segments on the bg thread (if the `seq_num` reaches X -> decrease) -> see LSM-Trees
- Scan the segments efficiently on a read-through (key not in memory)
- KvStore methods separated -> clean up
- Serializing into an on-disk structure -> bytes?
- Error propagation with `Results`
  - KvError
- _More effecive way of getting the latest segment?_

### Crypto

- Encrypted logs / segments
- Decrpyted in memory
- ...
