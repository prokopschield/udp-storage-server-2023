use super::super::error::{to_error, UssResult};

pub fn decompress(data: &[u8], outlen: usize) -> UssResult<Vec<u8>> {
    let mut decompressor = libdeflater::Decompressor::new();
    let mut out: Vec<u8> = Vec::with_capacity(outlen);

    unsafe {
        // safe because we know the Vec's capacity
        out.set_len(outlen);
    }

    match libdeflater::Decompressor::deflate_decompress(&mut decompressor, data, out.as_mut_slice())
    {
        Ok(length) => {
            if length < outlen {
                unsafe {
                    // safe because we're decreasing Vec size
                    out.set_len(length);
                }
            }

            Ok(out)
        }
        Err(err) => return to_error(err),
    }
}
