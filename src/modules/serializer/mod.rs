use crate::{store::*, *};
use serde::{de::DeserializeOwned, Serialize};

pub fn serialize<T: Serialize>(arg: &T, lake: &mut DataLake) -> UssResult<String> {
    let vec = bitcode::serialize(arg).map_err(to_error)?;

    Ok(unsafe {
        String::from_utf8_unchecked(if vec.len() < 36 {
            base64::encode(&vec)
        } else {
            lake.put(&vec)?.header.hash.to_vec()
        })
    })
}

pub fn hash_to_bytes(arg: &[u8], lake: &mut DataLake) -> Vec<u8> {
    if arg.len() < 50 {
        return base64::decode(arg);
    }

    if arg.len() == 50 {
        let hash: Option<[u8; 50]> = match arg.try_into() {
            Ok(hash) => Some(hash),
            _ => None,
        };

        if let Some(hash) = hash {
            if let Some(chunk) = lake.get(&hash) {
                if let Some(data) = chunk.read().ok() {
                    return data.to_vec();
                }
            }
        }
    }

    return arg.to_vec();
}

pub fn deserialize<T: DeserializeOwned>(arg: &[u8], lake: &mut DataLake) -> UssResult<T> {
    bitcode::deserialize(&hash_to_bytes(arg, lake)).map_err(to_error)
}
