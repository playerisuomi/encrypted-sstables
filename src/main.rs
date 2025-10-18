use anyhow::Result;
use enc_kv_store::store::KvStore;
use once_cell::sync::Lazy;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::{
    env::{self},
    fs::{self},
};

static DEFAULT: Lazy<String> = Lazy::new(|| String::from("password"));

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let password = args.get(1).unwrap_or(&DEFAULT);
    let curr_dir = env::current_dir().expect("invalid curr dir");

    let latest_segment = get_dir_segment_count(curr_dir.as_path()).expect("segment count");

    let hm: Arc<KvStore<String>> = Arc::new(KvStore::new(
        password.to_owned(),
        latest_segment,
        curr_dir.clone(),
    )?);

    let bg_hm = Arc::clone(&hm);
    thread::spawn(move || {
        if let Err(err) = bg_hm.run_bg_thread() {
            eprintln!("{err}")
        }
    });
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

#[cfg(test)]
mod tests {}
