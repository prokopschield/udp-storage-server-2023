use serde::{de::DeserializeOwned, Serialize};
use std::rc::Rc;

use super::*;

pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Rc<Node<K, V>>),
    Leaf(Rc<Leaf<K, V>>),
}

pub struct NodeEntry<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    key: u32,
    child: NodeChild<K, V>,
}

pub struct Node<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    depth: usize,
    entries: Vec<NodeEntry<K, V>>,
}
