use std::io::Read;
use std::io::Seek;
use std::os::windows::fs::FileExt;
use std::{collections::BTreeMap, fs::File, path::PathBuf};

use anyhow::Result;

use crate::FOOTER_SIZE;

#[derive(Debug, Clone)]
pub struct SegmentIter {
    curr: usize,
    seg_ids: Vec<usize>,
    origin: PathBuf,
}

impl SegmentIter {
    pub fn new(seg_ids: Vec<usize>, origin_path: PathBuf) -> Self {
        Self {
            curr: seg_ids.len(),
            seg_ids: seg_ids,
            origin: origin_path,
        }
    }

    pub fn find_key_in_segments(self, key: &str) -> Result<Option<String>> {
        for mut seg in self {
            let mut key_offset: u64 = 0;
            for (k, v) in seg.idx.iter() {
                match k.as_str().cmp(key) {
                    std::cmp::Ordering::Greater => break,
                    std::cmp::Ordering::Equal => {
                        key_offset = *v;
                        break;
                    }
                    std::cmp::Ordering::Less => {
                        key_offset = *v;
                    }
                }
            }
            if let Ok(Some(v)) = seg.search(key, key_offset) {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

impl Iterator for SegmentIter {
    type Item = SegmentFile<String, u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.curr.checked_sub(1) {
            self.curr = curr;
        } else {
            return None;
        }

        let seg_path = self
            .origin
            .join(format!("segment_{}.sstable", self.seg_ids[self.curr]));

        if let Ok(mut seg_file) = File::open(seg_path) {
            let (offset, size) = SegmentFile::parse_footer(&mut seg_file).unwrap();
            let mut idx: Vec<u8> = vec![0; size.try_into().unwrap()];

            let n = seg_file.seek_read(&mut idx, offset).unwrap();
            assert_eq!(size, n.try_into().unwrap());

            let mut seg = SegmentFile::new(offset, seg_file);
            seg.populate_index(idx).unwrap();

            return Some(seg);
        }
        None
    }
}

#[derive(Debug)]
pub struct SegmentFile<K, V> {
    seg_handle: File,
    idx_offset: u64,
    idx: BTreeMap<K, V>,
}

impl SegmentFile<String, u64> {
    pub fn new(offset: u64, seg_file: File) -> Self {
        Self {
            idx: BTreeMap::new(),
            idx_offset: offset,
            seg_handle: seg_file,
        }
    }

    fn search(&mut self, k: &str, key_offset: u64) -> Result<Option<String>> {
        self.seg_handle.seek(std::io::SeekFrom::Start(key_offset))?;
        loop {
            if self.seg_handle.stream_position()? >= self.idx_offset {
                return Ok(None);
            }

            let mut key_len_bytes: [u8; 4] = [0; 4];
            self.seg_handle.read_exact(&mut key_len_bytes)?;

            let key_len = u32::from_be_bytes(key_len_bytes);
            let mut key_bytes = vec![0; key_len as usize];
            self.seg_handle.read_exact(&mut key_bytes)?;

            let mut value_len_bytes: [u8; 4] = [0; 4];
            self.seg_handle.read_exact(&mut value_len_bytes)?;

            let value_len = u32::from_be_bytes(value_len_bytes);
            let mut value_bytes = vec![0; value_len as usize];
            self.seg_handle.read_exact(&mut value_bytes)?;

            // Strings assumed for now?
            let key = String::from_utf8(key_bytes)?;
            let value = String::from_utf8(value_bytes)?;

            if key.as_str() == k {
                return Ok(Some(value));
            }
            if key.as_str() > k {
                return Ok(None);
            }
        }
    }

    fn populate_index(&mut self, idx_bytes: Vec<u8>) -> Result<()> {
        let mut idx_cursor = &idx_bytes[..];
        while !idx_cursor.is_empty() {
            let mut key_len_bytes: [u8; 4] = [0; 4];
            idx_cursor.read_exact(&mut key_len_bytes)?;

            let key_len = u32::from_be_bytes(key_len_bytes);
            let mut key_bytes = vec![0; key_len as usize];
            idx_cursor.read_exact(&mut key_bytes)?;

            let mut offset_bytes: [u8; 8] = [0; 8];
            idx_cursor.read_exact(&mut offset_bytes)?;

            let offset = u64::from_be_bytes(offset_bytes);
            self.idx
                .insert(String::from_utf8(key_bytes)?.into(), offset);
        }
        Ok(())
    }

    fn parse_footer(seg_file: &mut File) -> Result<(u64, u64)> {
        let seg_size = seg_file.metadata()?.len();

        let mut footer_bytes: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        let footer_offset = (seg_size as usize - FOOTER_SIZE) as u64;
        seg_file.seek_read(&mut footer_bytes[0..FOOTER_SIZE], footer_offset)?;

        let idx_offset = u64::from_be_bytes(footer_bytes[0..FOOTER_SIZE / 2].try_into()?);
        let idx_size = u64::from_be_bytes(footer_bytes[FOOTER_SIZE / 2..FOOTER_SIZE].try_into()?);

        Ok((idx_offset, idx_size))
    }
}
