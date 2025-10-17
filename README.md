# enc-kv-store

A learning experience for someone interested in Rust, systems concepts and cryptography.

### Testing

```
cargo run -- password
```

### Notes

- Only able to access the segments that were decrypted with a certain password!
- SSTables -> background thread(pool) to merge?
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)
  - Sleeps (_Condvar_) and listens for signals (_mpsc_)

### Next

- **Logging**
- **Serde byte serialization**
- _Merge segments_
- _App module for stdin loop?_
- _Verify the loaded key against the saved key -> save the key hash?_
- Tests to populate the memtable (and write segments) / encryption
  - Edge case functionality?
