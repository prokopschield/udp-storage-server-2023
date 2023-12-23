use super::super::error::{to_error, UssResult};

// 4096 (max item size) + 5 bytes (per libdeflater)
const COMPRESSION_VEC_CAPACITY: usize = 4111;

pub struct CompressorCollectionNode {
    compressor: libdeflater::Compressor,
    next: Option<Box<CompressorCollectionNode>>,
}

pub struct CompressorCollection {
    count: usize,
    first: Option<Box<CompressorCollectionNode>>,
}

pub fn alloc_compressor() -> libdeflater::Compressor {
    let level = libdeflater::CompressionLvl::best();
    let compressor = libdeflater::Compressor::new(level);

    return compressor;
}

impl CompressorCollection {
    pub fn new() -> CompressorCollection {
        CompressorCollection {
            count: 0,
            first: None,
        }
    }

    pub fn pop(&mut self) -> libdeflater::Compressor {
        let taken = self.first.take();

        match taken {
            None => {
                return alloc_compressor();
            }
            Some(mut boxed) => {
                self.first = boxed.next.take();
                self.count -= 1;

                return boxed.compressor;
            }
        }
    }

    pub fn push(&mut self, compressor: libdeflater::Compressor) -> &mut CompressorCollection {
        let node = CompressorCollectionNode {
            compressor,
            next: self.first.take(),
        };

        let boxed = Box::new(node);

        self.first = Some(boxed);
        self.count += 1;

        return self;
    }

    pub fn compress(self: &mut CompressorCollection, data: &[u8]) -> UssResult<Vec<u8>> {
        let mut compressor = self.pop();
        let mut out: Vec<u8> = Vec::with_capacity(COMPRESSION_VEC_CAPACITY);

        unsafe {
            // otherwise deflate will fail, this is safe because we don't care about the u8 content of the Vec
            out.set_len(COMPRESSION_VEC_CAPACITY);
        }

        let result = match libdeflater::Compressor::deflate_compress(
            &mut compressor,
            data,
            out.as_mut_slice(),
        ) {
            Ok(length) => {
                unsafe {
                    // this is safe because we know the Vec's capacity
                    if length < COMPRESSION_VEC_CAPACITY {
                        out.set_len(length);
                    }
                }
                Ok(out)
            }
            Err(err) => return to_error(err),
        };

        self.push(compressor);

        return result;
    }
}

pub fn init_compressor_collection() -> CompressorCollection {
    return CompressorCollection::new();
}

pub fn compress(data: &[u8]) -> UssResult<Vec<u8>> {
    return CompressorCollection::new().compress(data);
}
