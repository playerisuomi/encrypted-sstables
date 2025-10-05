# enc-kv-store

### Notes

- SSTables -> background thread(pool) to merge?
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)
  - Sleeps (_Condvar_) and listens for signals (_mpsc_)

### Next

- _Merge segments_
- KvStore methods separated -> clean up
- Error propagation with `Results`
  - KvError
- Tests to populate the memtable (and write segments)
- _More effecive way of getting the latest segment?_

### Crypto

- Encrypted logs / segments (articles on this?)
- Decrpyted in memory
- ...
