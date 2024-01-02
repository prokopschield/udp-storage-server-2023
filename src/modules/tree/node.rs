use super::*;
use crate::{hasher::checksum_u32, serializer::*, store::DataLake, *};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{Arc, Mutex},
};

pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Rc<Node<K, V>>),
    Leaf(Rc<Leaf<K, V>>),
    Lazy(Rc<Lazy<K, V>>),
}

impl<K, V> Clone for NodeChild<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn clone(&self) -> Self {
        match self {
            NodeChild::Node(rc) => NodeChild::Node(rc.clone()),
            NodeChild::Leaf(rc) => NodeChild::Leaf(rc.clone()),
            NodeChild::Lazy(rc) => NodeChild::Lazy(rc.clone()),
        }
    }
}

pub struct NodeEntry<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    key: u32,
    child: NodeChild<K, V>,
}

impl<K, V> Clone for NodeEntry<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            child: self.child.clone(),
        }
    }
}

pub struct Node<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    depth: usize,
    entries: Vec<NodeEntry<K, V>>,
    lake: Arc<Mutex<DataLake>>,
}

impl<K, V> Clone for Node<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn clone(&self) -> Self {
        Self {
            depth: self.depth,
            entries: self.entries.clone(),
            lake: self.lake.clone(),
        }
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Node<K, V> {
    pub fn new_from_props(
        lake: Arc<Mutex<DataLake>>,
        depth: usize,
        entries: Vec<NodeEntry<K, V>>,
    ) -> Self {
        Self {
            depth,
            entries,
            lake,
        }
    }

    pub fn new_with_depth(lake: Arc<Mutex<DataLake>>, depth: usize) -> Self {
        Self::new_from_props(lake, depth, vec![])
    }

    pub fn new(lake: Arc<Mutex<DataLake>>) -> Self {
        Self::new_with_depth(lake, 0)
    }

    pub fn hash(&self) -> UssResult<String> {
        if self.depth == 0 {
            let mut children: Vec<(u32, String)> = Vec::new();

            for entry in self.entries.iter() {
                match &entry.child {
                    NodeChild::Lazy(lazy) => {
                        let node = lazy.load()?;

                        for item in NodeIterator::from(node) {
                            match item {
                                Ok(leaf) => children.push((leaf.key_u32(), leaf.hash()?)),
                                Err(err) => return Err(err),
                            }
                        }
                    }

                    NodeChild::Node(node) => {
                        for item in NodeIterator::from(node.clone()) {
                            match item {
                                Ok(leaf) => children.push((leaf.key_u32(), leaf.hash()?)),
                                Err(err) => return Err(err),
                            }
                        }
                    }

                    NodeChild::Leaf(leaf) => children.push((leaf.key_u32(), leaf.hash()?)),
                }
            }

            let mut lock = self.lake.lock().map_err(to_error)?;

            serialize(&(self.depth, children), &mut lock)
        } else {
            let mut children: Vec<(u32, Rc<String>)> = Vec::new();

            for entry in self.entries.iter() {
                let str = match &entry.child {
                    NodeChild::Lazy(lazy) => lazy.hash(),
                    NodeChild::Node(node) => Rc::from(node.hash()?),
                    NodeChild::Leaf(_) => return Err(to_error("Node::hash(): unexpected leaf")),
                };

                children.push((entry.key, str))
            }

            let arg = (
                self.depth,
                children
                    .iter()
                    .map(|(key, child)| (*key, child.as_str()))
                    .collect::<Vec<(u32, &str)>>(),
            );

            let mut lock = self.lake.lock().map_err(to_error)?;

            serialize(&arg, &mut lock)
        }
    }

    pub fn from_hash(hash: &[u8], lake: Arc<Mutex<DataLake>>) -> UssResult<Self> {
        let mut lock = lake.lock().map_err(to_error)?;
        let (depth, children) = deserialize::<(usize, Vec<(u32, String)>)>(hash, &mut lock)?;

        let mut entries: Vec<NodeEntry<K, V>> = Vec::with_capacity(children.len());

        if depth == 0 {
            for (check_key, child) in children.into_iter() {
                let leaf = Leaf::<K, V>::from_hash(child.as_bytes(), lake.clone())?;
                let key = leaf.key_u32();
                let child = NodeChild::Leaf::<K, V>(Rc::from(leaf));
                let entry = NodeEntry { child, key };

                if check_key != key {
                    return Err(UssError::DynamicError(format!("Error while deserializing node: Invalid Node::from_hash({}): leaf key {} does not match entry key {}", String::from_utf8_lossy(hash), key, check_key)));
                }

                entries.push(entry);
            }
        } else {
            for (key, child) in children.into_iter() {
                let lazy = Lazy::<K, V>::from_hash(child, lake.clone());
                let child = NodeChild::Lazy::<K, V>(Rc::from(lazy));
                let entry = NodeEntry { child, key };

                entries.push(entry);
            }
        }

        drop(lock);

        Ok(Node {
            depth,
            entries,
            lake,
        })
    }

    pub fn get_internal_offset(&self, key: u32) -> usize {
        let mut index = 0;

        for item in self.entries.iter() {
            if key >= item.key {
                index = item.key
            } else {
                break;
            }
        }

        index as usize
    }

    pub fn set_rc_leaf(&self, leaf: Rc<Leaf<K, V>>) -> UssResult<Self> {
        let key = leaf.key_u32();

        if self.entries.len() == 0 {
            return Ok(Self {
                depth: 0,
                entries: vec![NodeEntry {
                    child: NodeChild::Leaf(leaf),
                    key,
                }],
                lake: self.lake.clone(),
            });
        }

        let offset = self.get_internal_offset(key);

        if self.depth == 0 {
            let entry = &self.entries[offset];

            if entry.key == key {
                let mut entries = Vec::with_capacity(self.entries.len());

                entries.extend_from_slice(&self.entries[0..offset]);

                entries.push(NodeEntry {
                    child: NodeChild::Leaf(leaf),
                    key,
                });

                if self.entries.len() > offset + 1 {
                    entries.extend_from_slice(&self.entries[offset + 1..self.entries.len()]);
                }

                return Ok(Self {
                    depth: self.depth,
                    entries,
                    lake: self.lake.clone(),
                });
            } else {
                let mut entries = Vec::with_capacity(self.entries.len() + 1);

                entries.extend_from_slice(&self.entries[0..offset]);

                entries.push(NodeEntry {
                    child: NodeChild::Leaf(leaf),
                    key,
                });

                entries.extend_from_slice(&self.entries[offset..self.entries.len()]);

                return Ok(Self {
                    depth: self.depth,
                    entries,
                    lake: self.lake.clone(),
                });
            }
        }

        if let NodeChild::Node(node) = &self.entries[offset].child {
            let mut entries = Vec::with_capacity(self.entries.len());

            entries.extend_from_slice(&self.entries[0..offset]);

            entries.push(NodeEntry {
                child: NodeChild::Node(Rc::from(node.set_rc_leaf(leaf)?)),
                key,
            });

            if self.entries.len() > offset + 1 {
                entries.extend_from_slice(&self.entries[offset + 1..self.entries.len()]);
            }

            return Ok(Self {
                depth: self.depth,
                entries,
                lake: self.lake.clone(),
            });
        } else {
            // invalid tree, attempt to rectify the situation

            let mut entries = Vec::with_capacity(self.entries.len());

            entries.extend_from_slice(&self.entries[0..offset]);

            entries.push(NodeEntry {
                child: NodeChild::Node(Rc::from(Self::new_from_props(
                    self.lake.clone(),
                    self.depth - 1, // self.depth not 0, we wouldn't get here if it were 0
                    vec![
                        self.entries[offset].clone(),
                        NodeEntry {
                            child: NodeChild::Leaf(Rc::from(leaf)),
                            key,
                        },
                    ],
                ))),
                key,
            });

            if self.entries.len() > offset + 1 {
                entries.extend_from_slice(&self.entries[offset + 1..self.entries.len()]);
            }

            return Ok(Self {
                depth: self.depth,
                entries,
                lake: self.lake.clone(),
            });
        }
    }

    pub fn set_leaf(&self, leaf: Leaf<K, V>) -> UssResult<Self> {
        self.set_rc_leaf(Rc::from(leaf))
    }

    pub fn set(&self, key: K, val: V) -> UssResult<Self> {
        self.set_leaf(Leaf::from_kv(key, val, self.lake.clone())?)
    }

    pub fn get_ge_by_u32(&self, key: u32) -> UssResult<Option<Rc<Leaf<K, V>>>> {
        if self.entries.len() == 0 {
            return Ok(None);
        }

        let offset = self.get_internal_offset(key);
        let child = &self.entries[offset].child;

        match child {
            NodeChild::Node(node) => node.get_ge_by_u32(key),
            NodeChild::Leaf(leaf) => Ok(Some(leaf.clone())),
            NodeChild::Lazy(lazy) => lazy.load()?.get_ge_by_u32(key),
        }
    }

    pub fn get_by_u32(&self, key: u32) -> UssResult<Option<Rc<Leaf<K, V>>>> {
        let leaf = self.get_ge_by_u32(key)?;

        if let Some(leaf) = leaf {
            if leaf.key_u32() == key {
                return Ok(Some(leaf));
            }
        }

        Ok(None)
    }

    pub fn key_to_u32(&self, key: &K) -> UssResult<u32> {
        let mut lock = self.lake.lock().map_err(to_error)?;
        let hash = serializer::serialize(key, &mut lock)?;
        let key = checksum_u32(hash.as_bytes(), hash.len() as u32);

        Ok(key)
    }

    pub fn get_ge(&self, key: &K) -> UssResult<Option<Rc<Leaf<K, V>>>> {
        self.get_ge_by_u32(self.key_to_u32(key)?)
    }

    pub fn get(&self, key: &K) -> UssResult<Option<Rc<Leaf<K, V>>>> {
        self.get_by_u32(self.key_to_u32(key)?)
    }
}

pub enum LazyContent<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    None,
    Node(Rc<Node<K, V>>),
    Lake(Arc<Mutex<DataLake>>),
}

pub struct Lazy<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    content: Cell<LazyContent<K, V>>,
    hash: Rc<String>,
}

impl<K, V> Lazy<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn load(&self) -> UssResult<Rc<Node<K, V>>> {
        match self.content.replace(LazyContent::None) {
            LazyContent::None => Err(UssError::StaticError("tree: empty Lazy node")),
            LazyContent::Node(node) => {
                self.content.replace(LazyContent::Node(node.clone()));
                Ok(node)
            }
            LazyContent::Lake(lake) => {
                let node = Rc::from(Node::from_hash(self.hash.as_bytes(), lake)?);
                self.content.replace(LazyContent::Node(node.clone()));
                Ok(node)
            }
        }
    }

    pub fn hash(&self) -> Rc<String> {
        self.hash.clone()
    }

    pub fn from_rc_hash(hash: Rc<String>, lake: Arc<Mutex<DataLake>>) -> Self {
        Self {
            content: Cell::from(LazyContent::Lake(lake)),
            hash,
        }
    }

    pub fn from_hash(hash: String, lake: Arc<Mutex<DataLake>>) -> Self {
        Self::from_rc_hash(Rc::from(hash), lake)
    }
}

pub struct NodeIterator<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    node: Rc<Node<K, V>>,
    iter: Option<Box<NodeIterator<K, V>>>,
    index: usize,
}

impl<K, V> Iterator for NodeIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = UssResult<Rc<Leaf<K, V>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut iter) = self.iter.take() {
            if let Some(next) = iter.next() {
                self.iter.replace(iter);
                return Some(next);
            }
        }

        if self.index > self.node.entries.len() {
            return None;
        }

        let child = &self.node.entries[self.index].child;

        self.index += 1;

        match child {
            NodeChild::Lazy(lazy) => {
                let node = match lazy.load() {
                    Ok(node) => node,
                    Err(err) => return Some(Err(err)),
                };

                self.iter = Some(Box::from(NodeIterator::from(node)));
                self.next()
            }
            NodeChild::Node(node) => {
                self.iter = Some(Box::from(NodeIterator::from(node.clone())));
                self.next()
            }
            NodeChild::Leaf(leaf) => Some(Ok(leaf.clone())),
        }
    }
}

impl<K, V> NodeIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pub fn from(node: Rc<Node<K, V>>) -> Self {
        Self {
            node,
            iter: None,
            index: 0,
        }
    }
}
