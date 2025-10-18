use crate::FOOTER_SIZE;
use crate::encryption::Decrypter;
use crate::encryption::DefaultDecrypter;
use anyhow::Result;
use argon2::password_hash::SaltString;
use std::io::Read;
use std::io::Seek;
use std::os::windows::fs::FileExt;
use std::{collections::BTreeMap, fs::File, path::PathBuf};

#[derive(Debug, Clone)]
pub struct SegmentIter {
    password: String,
    curr: usize,
    seg_ids: Vec<usize>,
    origin: PathBuf,
}

impl SegmentIter {
    pub fn new(seg_ids: Vec<usize>, origin_path: PathBuf, password: String) -> Self {
        Self {
            curr: seg_ids.len(),
            password: password,
            seg_ids: seg_ids,
            origin: origin_path,
        }
    }

    pub fn find_key_in_segments(self, key: &str) -> Result<Option<String>> {
        for seg in self {
            let mut key_offset: u64 = 0;
            let mut seg = seg?;
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

    fn load_segment(&self, seg_path: &PathBuf) -> Result<SegmentFile> {
        let mut seg_file = File::open(seg_path)?;
        let (offset, size, salt) = SegmentFile::parse_footer(&mut seg_file)?;
        let mut idx: Vec<u8> = vec![0; size.try_into()?];

        let n = seg_file.seek_read(&mut idx, offset)?;
        assert_eq!(size, n.try_into()?);

        let file_decrypter = DefaultDecrypter::new(self.password.clone(), salt)?;
        let mut seg = SegmentFile::new(offset, seg_file, file_decrypter);
        seg.populate_index(idx)?;

        Ok(seg)
    }
}

impl Iterator for SegmentIter {
    type Item = Result<SegmentFile>;

    // Note: assumes a continuous sequence of segments without "gaps"
    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.curr.checked_sub(1)?;
        self.curr = curr;

        let seg_path = self
            .origin
            .join(format!("segment_{}.sstable", self.seg_ids[self.curr]));

        Some(self.load_segment(&seg_path))
    }
}

#[derive(Debug)]
pub struct SegmentFile {
    seg_handle: File,
    idx_offset: u64,
    idx: BTreeMap<String, u64>,
    decrypter: DefaultDecrypter,
}

impl SegmentFile {
    pub fn new(offset: u64, seg_file: File, decrypter: DefaultDecrypter) -> Self {
        Self {
            idx: BTreeMap::new(),
            idx_offset: offset,
            decrypter: decrypter,
            seg_handle: seg_file,
        }
    }

    fn search<V>(&mut self, k: &str, key_offset: u64) -> Result<Option<V>>
    where
        V: bincode::Decode<()>,
    {
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

            let mut nonce_bytes: [u8; 12] = [0; 12];
            self.seg_handle.read_exact(&mut nonce_bytes)?;

            let mut enc_bytes_len: [u8; 4] = [0; 4];
            self.seg_handle.read_exact(&mut enc_bytes_len)?;

            let enc_len = u32::from_be_bytes(enc_bytes_len);
            let mut enc_bytes = vec![0; enc_len as usize];
            self.seg_handle.read_exact(&mut enc_bytes)?;

            let key = String::from_utf8(key_bytes.clone())?;
            if key.as_str() == k {
                let plaintext_slice =
                    self.decrypter
                        .decrypt(&mut enc_bytes, nonce_bytes, &mut key_bytes)?;
                let value: (V, usize) =
                    bincode::decode_from_slice(&plaintext_slice, bincode::config::standard())?;
                return Ok(Some(value.0));
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

    fn parse_footer(seg_file: &mut File) -> Result<(u64, u64, SaltString)> {
        let seg_size = seg_file.metadata()?.len();

        let mut footer_bytes: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
        let footer_offset = (seg_size as usize - FOOTER_SIZE) as u64;
        seg_file.seek_read(&mut footer_bytes[0..FOOTER_SIZE], footer_offset)?;

        let idx_offset = u64::from_be_bytes(footer_bytes[0..8].try_into()?);
        let idx_size = u64::from_be_bytes(footer_bytes[8..16].try_into()?);

        let salt_bytes = &footer_bytes[16..];
        let salt = DefaultDecrypter::encode_salt_string(salt_bytes)?;

        Ok((idx_offset, idx_size, salt))
    }
}
