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

    pub fn key(&mut self) -> UssResult<Rc<K>> {
        if let Some(key) = self.key_val.clone() {
            return Ok(key);
        }

        let mut lock = self.lake.lock().map_err(to_error)?;
        let key: K = deserialize(self.key_ref.as_bytes(), &mut lock)?;
        let rc = Rc::new(key);

        self.key_val = Some(rc.clone());

        return Ok(rc);
    }

    pub fn value(&mut self) -> UssResult<Rc<V>> {
        if let Some(value) = self.val_val.clone() {
            return Ok(value);
        }

        let mut lock = self.lake.lock().map_err(to_error)?;
        let value: V = deserialize(self.val_ref.as_bytes(), &mut lock)?;
        let rc = Rc::new(value);

        self.val_val = Some(rc.clone());

        return Ok(rc);
    }
}

pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Vec<Node<K, V>>),
    Leaf(Vec<Leaf<K, V>>),
}

pub struct Node<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    depth: usize,
    keys: Vec<K>,
    children: NodeChild<K, V>,
}
