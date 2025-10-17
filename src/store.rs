use base64::Engine;
use base64::prelude::BASE64_STANDARD;

use crate::encryption::DecryptError;
use crate::encryption::Decrypter;
use crate::encryption::DefaultDecrypter;
use crate::encryption::DefaultEncrypter;
use crate::encryption::EncryptError;
use crate::encryption::Encrypter;
use core::fmt;
use std::io::Read;
use std::io::Write;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    fs::File,
    str::FromStr,
};

#[derive(Debug, Clone)]
pub struct KvError(pub &'static str);

impl Display for KvError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "KvStore: Context '{}'", self.0)
    }
}

// For now
impl From<std::io::Error> for KvError {
    fn from(_: std::io::Error) -> Self {
        KvError("IO error")
    }
}

impl From<EncryptError> for KvError {
    fn from(_value: EncryptError) -> Self {
        KvError("Failed to encrypt")
    }
}

impl From<DecryptError> for KvError {
    fn from(_value: DecryptError) -> Self {
        KvError("Failed to decrypt")
    }
}

#[derive(Debug, Clone)]
pub struct KvStore<V> {
    pub memtable: BTreeMap<String, V>,
    encrypter: DefaultEncrypter,
}

impl<V> KvStore<V>
where
    V: FromStr, // Serialize + Deserialize
{
    pub fn new(encrypter: DefaultEncrypter /* for now, just default */) -> Self {
        Self {
            encrypter,
            memtable: BTreeMap::new(),
        }
    }

    pub fn sync_wal(&mut self, mut file: File, password: String) -> Result<(), KvError>
    where
        V: FromStr,
        <V as FromStr>::Err: std::fmt::Debug,
    {
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();

        for entry in buf.lines() {
            let cmd_seq: Vec<_> = entry.split_whitespace().collect();
            match cmd_seq[0] {
                "SET" => {
                    assert!(cmd_seq.len() == 5);
                    let mut key: Vec<u8> = Vec::from(cmd_seq[1].as_bytes());
                    let mut enc_string: Vec<u8> =
                        BASE64_STANDARD.decode(cmd_seq[2]).expect("decode value");
                    let nonce: Vec<u8> = BASE64_STANDARD.decode(cmd_seq[3]).expect("decode nonce");
                    let salt_bytes: Vec<u8> =
                        BASE64_STANDARD.decode(cmd_seq[4]).expect("decode nonce");

                    let enc_bytes = enc_string.as_mut_slice();
                    let nonce_bytes: [u8; 12] = nonce.try_into().expect("invalid nonce bytes");
                    let salt = DefaultDecrypter::encode_salt_string(
                        salt_bytes
                            .as_slice()
                            .try_into()
                            .expect("invalid salt bytes"),
                    )
                    .expect("salt string conversion");

                    let log_decrypter = DefaultDecrypter::new(password.clone(), salt);

                    let plaintext_bytes =
                        log_decrypter.decrypt(enc_bytes, nonce_bytes, &mut key)?;
                    let plaintext = String::from_utf8(plaintext_bytes.to_vec())
                        .expect("decrypted a not valid string");

                    if let Ok(value) = plaintext.parse::<V>() {
                        self.memtable.insert(cmd_seq[1].to_string(), value);
                    } else {
                        return Err(KvError("sync wal"));
                    }
                }
                _ => return Err(KvError("unknown cmd")),
            };
        }
        Ok(())
    }

    pub fn write_wal(&self, k: String, v: String, file: &mut File) -> Result<(), KvError>
    where
        V: FromStr,
        <V as FromStr>::Err: std::fmt::Debug,
    {
        let mut sealed_bytes = Vec::from(v.as_bytes());
        let mut nonce = self
            .encrypter
            .encrypt(&mut sealed_bytes, Some(k.as_bytes()))?;

        let mut salt_bytes: [u8; 16] = [0u8; 16];
        self.encrypter
            .decode_salt_bytes(&mut salt_bytes)
            .expect("salt decode");

        let encoded_string = BASE64_STANDARD.encode(&mut sealed_bytes);
        let nonce = BASE64_STANDARD.encode(&mut nonce);
        let salt_encoded = BASE64_STANDARD.encode(&mut salt_bytes);

        let log_entry = format!("SET {} {} {} {}\n", k, encoded_string, nonce, salt_encoded);
        let mut buf = Vec::from(log_entry.as_bytes());

        file.write_all(&mut buf)?;
        Ok(())
    }

    pub fn insert_parsed(&mut self, key: String, value: String) -> Result<(), KvError> {
        if let Ok(value) = value.parse::<V>() {
            self.memtable.insert(key, value);
        } else {
            return Err(KvError("insert"));
        }
        return Ok(());
    }
}
