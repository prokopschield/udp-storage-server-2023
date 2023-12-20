pub mod decoder;
pub mod encoder;

pub use decoder::decode;
pub use encoder::encode;

pub const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~_";
