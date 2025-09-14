# enc-kv-store

### Notes

- SSTables -> background thread(pool) to merge LSM-Trees
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)
  - Sleeps (_Condvar_) and listens for signals (_mpsc_)
