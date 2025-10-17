# enc-kv-store

A little learning project in Rust, systems concepts and cryptography.

## Testing

#### Commands

```powershell
cargo run -- password
```

#### Commands

```powershell
SET <key> <value>
GET <key>
```

## Notes

- Only able to access the segments that were decrypted with a certain password
- SSTables -> background thread(pool) to merge
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread handles compaction and flushing (job queue)

## Next

- **Logging**
- **Serde byte serialization**
- Configuration
- _Merge segments_
- _App module for stdin loop_
- _Verify the loaded key against the saved key -> save the key hash_
- Tests to populate the memtable (and write segments) / encryption
  - Edge case functionality
