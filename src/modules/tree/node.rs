use super::*;
use crate::store::DataLake;
use crate::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Rc<Node<K, V>>),
    Leaf(Rc<Leaf<K, V>>),
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
}
