use sha2::{Digest, Sha256};

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();

    hasher.update(data);

    let result = hasher.finalize();

    return result.into();
}

pub fn blake3(data: &[u8]) -> [u8; 32] {
    return *blake3::hash(data).as_bytes();
}

pub fn xor(a: [u8; 32], b: [u8; 32]) -> [u8; 32] {
    let mut result = [0; 32];

    for i in 0..32 {
        result[i] = a[i] ^ b[i];
    }

    return result;
}

pub fn checksum(data: &[u8], length: u32) -> [u8; 4] {
    let mut hash: u32 = length;

    for c in data.iter() {
        hash = (*c as u32)
            .wrapping_add(hash << 6)
            .wrapping_add(hash << 16)
            .wrapping_sub(hash);
    }

    return hash.to_ne_bytes();
}

pub fn hash(data: &[u8]) -> [u8; 50] {
    let length: u16 = data.len() as u16;
    let shasum = sha256(data);
    let blasum = blake3(data);
    let xored = xor(shasum, blasum);
    let checksum = checksum(&xored, length as u32);

    let mut vec: Vec<u8> = Vec::with_capacity(38);

    vec.extend_from_slice(&xored);
    vec.extend_from_slice(&checksum);
    vec.extend_from_slice(&length.to_le_bytes());

    let encoded = super::base64::encode(&vec);

    return encoded[..50].try_into().unwrap();
}

pub fn verify_hash_integrity(hash: &[u8; 50]) -> bool {
    let bytes = super::base64::decode(hash);
    let xored = &bytes[0..32];
    let length = bytes[36] as u32 + ((bytes[37] as u32) << 8);
    let checksum = checksum(xored, length);

    for i in 0..4 {
        if checksum[i] != bytes[i + 32] {
            return false;
        }
    }

    return true;
}
