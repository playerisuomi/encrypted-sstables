use rand::TryRngCore;
use ring::aead::{self, NonceSequence};

pub const MAX_MEMTABLE: usize = 1 << 2;
pub const FOOTER_SIZE: usize = 1 << 5;

pub mod segment;
pub mod store;

// Move to a encryption module!

#[derive(Clone, Debug)]
pub struct NoncePlaceholder {
    pub n: [u8; aead::NONCE_LEN],
}

impl NoncePlaceholder {
    pub fn new() -> Self {
        let mut nonce = [0u8; aead::NONCE_LEN];
        rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();
        Self { n: nonce }
    }

    pub fn from_bytes(bytes: [u8; aead::NONCE_LEN]) -> Self {
        Self { n: bytes }
    }
}

impl NonceSequence for NoncePlaceholder {
    fn advance(&mut self) -> std::result::Result<aead::Nonce, ring::error::Unspecified> {
        Ok(aead::Nonce::assume_unique_for_key(self.n))
    }
}
