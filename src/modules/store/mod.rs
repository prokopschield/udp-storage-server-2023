use super::error::{UssError, UssResult};

#[repr(C)]
pub struct DataChunkHeader {
    hash: [u8; 50],
    uncompressed_length: u16,
    compressed_length: u16,
}

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
}
