use std::collections::HashMap;

use super::{compression::decompress, error::*, mapping::*};

#[repr(C)]
pub struct DataChunkHeader {
    hash: [u8; 50],
    uncompressed_length: u16,
    compressed_length: u16,
}

const HEADER_SIZE: usize = std::mem::size_of::<DataChunkHeader>();

pub struct DataChunk {
    header: DataChunkHeader,
    mapping: &'static MemoryMapping,
    offset: u32,
}

pub fn offset_to_data_offset(offset: u32) -> usize {
    (offset as usize) << 8
}

impl DataChunk {
    pub fn at(mapping: &'static MemoryMapping, offset: u32) -> UssResult<Self> {
        let offset_real = offset_to_data_offset(offset);

        let mapping_location = mapping.roref.as_ptr() as usize;
        let offset_location = offset_real + mapping_location;
        let pointer = offset_location as *const DataChunkHeader;

        let header: DataChunkHeader = unsafe { std::ptr::read(pointer) };

        Ok(Self {
            header,
            mapping,
            offset,
        })
    }

    pub fn read_compressed(&self) -> UssResult<&[u8]> {
        let offset_real = offset_to_data_offset(self.offset);
        let mapping_location = self.mapping.roref.as_ptr() as usize;
        let offset_location = offset_real + mapping_location + HEADER_SIZE;
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

struct DataLakeHeader {
    magic: [u8; 8],
    data_size: u32,
    index_mod: u32,
    data_offset: u64,
    index_offset: u64,
    data_next: u32,
}

struct DataLake {
    data: MemoryMapping,
    chunks: HashMap<[u8; 50], DataChunk>,
    header: DataLakeHeader,
}

impl DataLake {
    pub fn load(filename: &str, readonly: bool) -> UssResult<DataLake> {
        let data_map = if readonly {
            create_ro_mapping(filename)?
        } else {
            create_rw_mapping(filename)?
        };

        let header_ptr = data_map.roref.as_ptr() as *const DataLakeHeader;
        let header: DataLakeHeader = unsafe { std::ptr::read(header_ptr) };

        Ok(DataLake {
            data: data_map,
            chunks: HashMap::new(),
            header,
        })
    }
}
