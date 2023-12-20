use super::ALPHABET;

fn decode_block(buffer: [u8; 4]) -> [u8; 3] {
    let mut value: u32 = 0;

    for i in 0..4 {
        let index = ALPHABET.iter().position(|&c| c == buffer[i]).map_or_else(
            || {
                if matches!(buffer[i], b'+' | b'-') {
                    62
                } else if matches!(buffer[i], b',' | b'/') {
                    63
                } else {
                    buffer[i]
                }
            },
            |v| v as u8,
        );

        value <<= 6;
        value += index as u32;
    }

    return [
        ((value >> 0x10) & 0xff) as u8,
        ((value >> 0x08) & 0xff) as u8,
        ((value >> 0x00) & 0xff) as u8,
    ];
}

pub fn decode(encoded: &[u8]) -> Vec<u8> {
    let size: usize = encoded.len() * 3 / 4;
    let mut rv: Vec<u8> = Vec::with_capacity(size);
    let mut index: usize = 0;

    for chunk in encoded.chunks(4) {
        let mut input_block: [u8; 4] = [0; 4];

        for (i, &byte) in chunk.iter().enumerate() {
            input_block[i] = byte;
        }

        let output_block: [u8; 3] = decode_block(input_block);

        for byte in output_block.iter() {
            if index < size {
                rv.push(*byte);
                index += 1;
            }
        }
    }

    return rv;
}
