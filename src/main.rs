use anyhow::Result;
use enc_kv_store::store::{KvError, KvStore};
use once_cell::sync::Lazy;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::{
    env::{self},
    fs::{self},
};

static DEFAULT: Lazy<String> = Lazy::new(|| String::from("password"));

fn main() -> Result<(), KvError> {
    let args: Vec<String> = env::args().collect();
    let password = args.get(1).unwrap_or(&DEFAULT);
    let curr_dir = env::current_dir().expect("curr dir");

    let latest_segment = get_dir_segment_count(env::current_dir().expect("curr dir").as_path())
        .expect("segment count");

    let hm: Arc<KvStore<String>> = Arc::new(KvStore::new(
        password.to_owned(),
        latest_segment,
        curr_dir.clone(),
    ));

    let bg_hm = Arc::clone(&hm);
    thread::spawn(move || bg_hm.run_bg_thread());
    hm.run()
}

fn get_dir_segment_count(dir_path: &Path) -> Result<usize> {
    Ok(fs::read_dir(dir_path)?
        .filter_map(|entry_result| {
            let entry = entry_result.ok()?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_str()?;
            if file_name_str.starts_with("segment_") {
                Some(())
            } else {
                None
            }
        })
        .count())
}

// #[cfg(test)]
// mod tests {
//     use enc_kv_store::encryption::DefaultEncrypter;
//     use enc_kv_store::store::KvStore;
//     use rand::Rng;
//     use rand::distr::{Alphanumeric, Uniform};

//     #[test]
//     fn test_random_str_type_once() {
//         let mut hm = KvStore::new(DefaultEncrypter::new(String::new()).unwrap());
//         // Custom function for "randoms" in KvStore?
//         let (k, v): (String, usize) = (
//             rand::rng()
//                 .sample_iter(&Alphanumeric)
//                 .take(1000)
//                 .map(char::from)
//                 .collect(),
//             rand::rng().sample(Uniform::new(10usize, 15).unwrap()),
//         );
//         hm.memtable.insert(k.clone(), v);
//         assert_eq!(&v, hm.memtable.get(&k).unwrap());
//     }

//     #[test]
//     fn test_random_str_store_and_retrieve() {
//         let hm: KvStore<String> = KvStore::new();
//     }

//     // Concurrency, on-disk saving, etc. to find edge cases?
//     // WALs
// }
