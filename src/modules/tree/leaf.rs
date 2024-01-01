use crate::{
    hasher::checksum_u32,
    serializer::{deserialize, serialize},
    store::DataLake,
    *,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Leaf<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    key_u32: u32,
    key_ref: String,
    key_val: Option<Rc<K>>,
    val_u32: u32,
    val_ref: String,
    val_val: Option<Rc<V>>,
    lake: Arc<Mutex<DataLake>>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Leaf<K, V> {
    pub fn from_hash(hash: &[u8], lake: Arc<Mutex<DataLake>>) -> UssResult<Self> {
        let mut lock = lake.lock().map_err(to_error)?;
        let (key_ref, val_ref) = deserialize::<(String, String)>(hash, &mut lock)?;
        let key_u32 = checksum_u32(key_ref.as_bytes(), key_ref.len() as u32);
        let val_u32 = checksum_u32(val_ref.as_bytes(), val_ref.len() as u32);

        drop(lock);

        Ok(Self {
            key_u32,
            key_ref,
            key_val: None,
            val_u32,
            val_ref,
            val_val: None,
            lake,
        })
    }

    pub fn from_kvrc(key: Rc<K>, val: Rc<V>, lake: Arc<Mutex<DataLake>>) -> UssResult<Self> {
        let mut lock = lake.lock().map_err(to_error)?;

        let key_ref = serialize(key.as_ref(), &mut lock)?;
        let val_ref = serialize(val.as_ref(), &mut lock)?;

        let key_u32 = checksum_u32(key_ref.as_bytes(), key_ref.len() as u32);
        let val_u32 = checksum_u32(val_ref.as_bytes(), val_ref.len() as u32);

        drop(lock);

        Ok(Self {
            key_u32,
            key_ref,
            key_val: Some(key),
            val_u32,
            val_ref,
            val_val: Some(val),
            lake,
        })
    }

    pub fn from_kv(key: K, value: V, lake: Arc<Mutex<DataLake>>) -> UssResult<Self> {
        Self::from_kvrc(Rc::from(key), Rc::from(value), lake)
    }

    pub fn hash(&self) -> UssResult<String> {
        let mut lock = self.lake.lock().map_err(to_error)?;

        serialize(&(self.key_ref.as_str(), self.val_ref.as_str()), &mut lock)
    }

    pub fn key(&self) -> UssResult<Rc<K>> {
        if let Some(key) = self.key_val.clone() {
            return Ok(key);
        }

        let mut lock = self.lake.lock().map_err(to_error)?;
        let key: K = deserialize(self.key_ref.as_bytes(), &mut lock)?;
        let rc = Rc::new(key);

        return Ok(rc);
    }

    pub fn key_cache(&mut self) -> UssResult<Rc<K>> {
        let rc = self.key()?;

        self.key_val = Some(rc.clone());

        return Ok(rc);
    }

    pub fn value(&self) -> UssResult<Rc<V>> {
        if let Some(value) = self.val_val.clone() {
            return Ok(value);
        }

        let mut lock = self.lake.lock().map_err(to_error)?;
        let value: V = deserialize(self.val_ref.as_bytes(), &mut lock)?;
        let rc = Rc::new(value);

        return Ok(rc);
    }

    pub fn value_cache(&mut self) -> UssResult<Rc<V>> {
        let rc = self.value()?;

        self.val_val = Some(rc.clone());

        return Ok(rc);
    }

    pub fn set_rc(&mut self, value: Rc<V>) -> UssResult<&mut Self> {
        let mut lock = self.lake.lock().map_err(to_error)?;

        let val_ref = serialize(value.as_ref(), &mut lock)?;
        let val_u32 = checksum_u32(val_ref.as_bytes(), val_ref.len() as u32);

        drop(lock);

        self.val_ref = val_ref;
        self.val_u32 = val_u32;
        self.val_val = Some(value);

        Ok(self)
    }

    pub fn set(&mut self, value: V) -> UssResult<&mut Self> {
        self.set_rc(Rc::from(value))
    }

    pub fn key_u32(&self) -> u32 {
        self.key_u32
    }

    pub fn key_ref(&self) -> &str {
        self.key_ref.as_ref()
    }

    pub fn key_val(&self) -> Option<Rc<K>> {
        self.key_val.clone()
    }

    pub fn val_u32(&self) -> u32 {
        self.val_u32
    }

    pub fn val_ref(&self) -> &str {
        self.val_ref.as_ref()
    }

    pub fn val_val(&self) -> Option<Rc<V>> {
        self.val_val.clone()
    }
}
