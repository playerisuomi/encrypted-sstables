# enc-kv-store

### Notes

- Only able to access the segments that were decrypted with a certain password!
- SSTables -> background thread(pool) to merge?
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)
  - Sleeps (_Condvar_) and listens for signals (_mpsc_)

### Next

- _Merge segments_
- **Serde byte serialization**
- Encryption into a module / layer consumed by the app / store?
- Logging
- _Verify the loaded key against the saved key -> save the key hash?_
- Tests to populate the memtable (and write segments)
  - Edge case functionality?
