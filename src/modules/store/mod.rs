use super::{
    compression::decompress,
    error::{UssError, UssResult},
};

#[repr(C)]
pub struct DataChunkHeader {
    hash: [u8; 50],
    uncompressed_length: u16,
    compressed_length: u16,
}

const HEADER_SIZE: usize = std::mem::size_of::<DataChunkHeader>();

pub struct DataChunk {
    header: DataChunkHeader,
    mapping: std::sync::Arc<std::sync::Mutex<memmap::MmapMut>>,
    offset: u32,
}

pub fn offset_to_data_offset(offset: u32) -> usize {
    (offset as usize) << 8
}

impl DataChunk {
    pub fn at(
        mapping: std::sync::Arc<std::sync::Mutex<memmap::MmapMut>>,
        offset: u32,
    ) -> UssResult<Self> {
        let offset_real = offset_to_data_offset(offset);

        let mut locked = match mapping.lock() {
            Ok(value) => value,
            Err(_) => return Err(UssError::MutexPoison),
        };

        let locked_location = locked.as_mut_ptr() as usize;
        let offset_location = offset_real + locked_location;
        let pointer = offset_location as *const DataChunkHeader;

        let header: DataChunkHeader = unsafe { std::ptr::read(pointer) };

        drop(locked);

        Ok(Self {
            header,
            mapping,
            offset,
        })
    }

    pub fn read_compressed(&self) -> UssResult<&[u8]> {
        let mut locked = match self.mapping.lock() {
            Ok(value) => value,
            Err(_) => return Err(UssError::MutexPoison),
        };

        let offset_real = offset_to_data_offset(self.offset);
        let locked_location = locked.as_mut_ptr() as usize;
        let offset_location = offset_real + locked_location + HEADER_SIZE;
        let pointer = offset_location as *const u8;
        let length = self.header.compressed_length as usize;

        let slice = unsafe { std::slice::from_raw_parts(pointer, length) };

        return Ok(slice);
    }

    pub fn read(&self) -> UssResult<Vec<u8>> {
        let compressed = self.read_compressed()?;
        let outlen = self.header.uncompressed_length as usize;
        let data = decompress(compressed, outlen)?;

        return Ok(data);
    }
}
