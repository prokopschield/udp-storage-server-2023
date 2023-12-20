use super::ALPHABET;

pub fn encode_block(buffer: [u8; 3]) -> [u8; 4] {
    return [
        ALPHABET[(buffer[0] >> 2) as usize],
        ALPHABET[(((buffer[0] & 0x3) << 4) | (buffer[1] >> 4)) as usize],
        ALPHABET[(((buffer[1] & 0xf) << 2) | (buffer[2] >> 6)) as usize],
        ALPHABET[(buffer[2] & 0x3f) as usize],
    ];
}

pub fn encode(buffer: &[u8]) -> Vec<u8> {
    let size: usize = buffer.len() * 4 / 3 + (if (buffer.len() % 3) != 0 { 1 } else { 0 });
    let mut rv: Vec<u8> = Vec::with_capacity(size);
    let mut index: usize = 0;

    for chunk in buffer.chunks(3) {
        let mut input_block: [u8; 3] = [0; 3];

        for (i, &byte) in chunk.iter().enumerate() {
            input_block[i] = byte;
        }

        let output_block: [u8; 4] = encode_block(input_block);

        for byte in output_block.iter() {
            if index < size {
                rv.push(*byte);
                index += 1;
            }
        }
    }

    return rv;
}
