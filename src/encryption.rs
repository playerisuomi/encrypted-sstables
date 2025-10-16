use std::sync::LazyLock;

use anyhow::Result;
use argon2::PasswordHasher;
use argon2::password_hash::Error as ArgonError;
use argon2::password_hash::rand_core::OsRng;
use argon2::{
    Argon2,
    password_hash::{Output, SaltString},
};
use rand::TryRngCore;
use ring::aead::BoundKey;
use ring::aead::{self, NonceSequence};
use ring::error::Unspecified;

static SALT: LazyLock<SaltString> = std::sync::LazyLock::new(|| SaltString::generate(&mut OsRng));

#[derive(Debug)]
pub enum EncryptError {
    KeyError(Unspecified),
}

impl From<Unspecified> for EncryptError {
    fn from(err: Unspecified) -> Self {
        EncryptError::KeyError(err)
    }
}

#[derive(Debug)]
pub enum DecryptError {
    KeyError(Unspecified),
    KeyMissing,
}

impl From<Unspecified> for DecryptError {
    fn from(err: Unspecified) -> Self {
        DecryptError::KeyError(err)
    }
}

#[derive(Debug)]
pub enum KeyGenError {
    HashError(ArgonError),
    HashMissing,
}

impl From<ArgonError> for KeyGenError {
    fn from(err: ArgonError) -> Self {
        KeyGenError::HashError(err)
    }
}

pub trait Encrypter {
    fn encrypt(
        &self,
        data_in_place: &mut Vec<u8>,
        aad: Option<&[u8]>,
    ) -> Result<[u8; 12], EncryptError>;
}

#[derive(Clone, Debug)]
pub struct DefaultEncrypter {
    pub password: String,
    pub key: Output,
    salt: SaltString,
}

impl DefaultEncrypter {
    pub fn new(password: String) -> Result<Self, KeyGenError> {
        let key = Argon2::default().hash_password(password.as_bytes(), SALT.as_salt())?;
        let key_output = key.hash.ok_or(KeyGenError::HashMissing)?;
        Ok(Self {
            key: key_output.to_owned(),
            password,
            salt: SALT.to_owned(),
        })
    }

    pub fn decode_salt_bytes<'a>(&self, buf: &'a mut [u8; 16]) -> Result<&'a [u8]> {
        Ok(self.salt.decode_b64(buf)?)
    }
}

impl Encrypter for DefaultEncrypter {
    fn encrypt(
        &self,
        data_in_place: &mut Vec<u8>,
        aad: Option<&[u8]>,
    ) -> Result<[u8; 12], EncryptError> {
        let nonce = NoncePlaceholder::new();
        let key_bytes = self.key.as_bytes();

        let u_key = aead::UnboundKey::new(&aead::AES_256_GCM, key_bytes)?;
        let mut sealing_key = aead::SealingKey::new(u_key, nonce.clone());

        assert!(aad.is_some());

        sealing_key.seal_in_place_append_tag(aead::Aad::from(aad.unwrap()), data_in_place)?;
        Ok(nonce.n.clone())
    }
}

pub trait Decrypter {
    fn decrypt<'a>(
        &self,
        enc_bytes: &'a mut [u8],
        nonce_bytes: [u8; 12],
        key_bytes: &mut Vec<u8>,
    ) -> Result<&'a mut [u8], DecryptError>;
}

#[derive(Debug, Clone)]
pub struct DefaultDecrypter {
    password: String,
    key: Option<Output>,
}

impl DefaultDecrypter {
    pub fn new(password: String, salt: SaltString) -> Self {
        let mut dc = Self {
            key: None,
            password: password,
        };
        dc.derive_key(salt).expect("derive key");
        dc
    }

    pub fn derive_key(&mut self, salt: SaltString) -> Result<(), KeyGenError> {
        let key = Argon2::default().hash_password(self.password.as_bytes(), salt.as_salt())?;
        self.key = Some(key.hash.ok_or(KeyGenError::HashMissing)?);
        Ok(())
    }

    pub fn encode_salt_string(salt_bytes: &[u8]) -> Result<SaltString> {
        Ok(SaltString::encode_b64(salt_bytes)?)
    }
}

impl Decrypter for DefaultDecrypter {
    fn decrypt<'a>(
        &self,
        enc_bytes: &'a mut [u8],
        nonce_bytes: [u8; 12],
        key_bytes: &mut Vec<u8>,
    ) -> Result<&'a mut [u8], DecryptError> {
        let nonce = NoncePlaceholder::from_bytes(nonce_bytes);
        let aad = aead::Aad::from(&key_bytes);

        if let Some(key) = self.key {
            let u_key = aead::UnboundKey::new(&aead::AES_256_GCM, key.as_bytes()).unwrap();
            let mut opening_key = aead::OpeningKey::new(u_key, nonce.clone());
            let plaintext = opening_key.open_in_place(aad, enc_bytes)?;
            return Ok(plaintext);
        }
        Err(DecryptError::KeyMissing)
    }
}

#[derive(Clone, Debug)]
pub struct NoncePlaceholder {
    // ref 'a n?
    pub n: [u8; aead::NONCE_LEN],
}

impl NoncePlaceholder {
    pub fn new() -> Self {
        let mut nonce = [0u8; aead::NONCE_LEN];
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce)
            .expect("unable to gen nonce");
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
