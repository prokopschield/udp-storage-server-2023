use super::*;
use crate::store::DataLake;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Rc<Node<K, V>>),
    Leaf(Rc<Leaf<K, V>>),
}

#[derive(Clone)]
pub struct NodeEntry<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    key: u32,
    child: NodeChild<K, V>,
}

#[derive(Clone)]
pub struct Node<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    depth: usize,
    entries: Vec<NodeEntry<K, V>>,
    lake: Arc<Mutex<DataLake>>,
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
}
