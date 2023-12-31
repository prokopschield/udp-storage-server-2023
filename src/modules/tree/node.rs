use serde::{de::DeserializeOwned, Serialize};

use super::*;

pub enum NodeChild<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    Node(Vec<Node<K, V>>),
    Leaf(Vec<Leaf<K, V>>),
}

pub struct Node<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    depth: usize,
    keys: Vec<K>,
    children: NodeChild<K, V>,
}
