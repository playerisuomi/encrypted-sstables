# enc-kv-store

A little learning project in Rust, systems concepts and cryptography. _Compacting feature is missing_.

## Testing

#### Running with flags (_set your own segment password_)

```powershell
make run PASS="password"
```

#### Commands

```powershell
SET <key> <value>
GET <key>
```

## Notes

- _Edited from elsewhere_
- Only able to access the segments that were decrypted with a certain password
- SSTables -> background thread(pool) to merge
- Main loop (on main thread) that listens for commands -> performs writes / reads with locks (`Arc<Mutex>`)
- Background thread should also handle compaction and flushing (job queue)

## Next

- **Logging**
- Configuration
- Error brevity
- _Merge segments_
- _App module for stdin loop_
- _Verify the loaded key against the saved key -> save the key hash_
- Tests to populate the memtable (and write segments) / encryption
  - Edge case functionality
