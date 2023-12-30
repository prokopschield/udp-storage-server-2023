pub mod sieve;

use super::{compression::decompress, error::*, mapping::*};
use std::{collections::HashMap, io::Write, rc::Rc};

#[derive(Copy, Clone)]
#[repr(C)]
pub struct DataChunkHeader {
    hash: [u8; 50],
    uncompressed_length: u16,
    compressed_length: u16,
}

const HEADER_SIZE: usize = std::mem::size_of::<DataChunkHeader>();

#[derive(Clone)]
pub struct DataChunk {
    header: DataChunkHeader,
    mapping: Rc<MemoryMapping>,
    offset: u32,
}

pub fn offset_to_data_offset(offset: u32) -> usize {
    (offset as usize) << 8
}

impl DataChunk {
    pub fn at(mapping: Rc<MemoryMapping>, offset: u32) -> UssResult<Self> {
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

#[repr(C)]
struct DataLakeHeader {
    // b"DataLake"
    magic: [u8; 8],
    // file size in bytes
    file_size: u64,
    // data size in 256-byte chunks
    data_size: u32,
    // offset where data starts in 256-byte chunks
    data_offset: u32,
    // next free 256-byte chunk
    data_next: u32,
    // index_offset: *mut u32 = (crc32(hash) % index_mod) + (index_offset >> 2)
    index_mod: u32,
    // index begins here (in 256-byte chunks)
    index_offset: u32,
    // index_offset << 6
    index_offset_u32: u32,
}

pub struct DataLake {
    data: Rc<MemoryMapping>,
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
            data: Rc::from(data_map),
            chunks: HashMap::new(),
            header,
        })
    }

    pub fn create(file_name: &str, file_size: u64) -> UssResult<DataLake> {
        if std::fs::metadata(file_name).is_ok() {
            return Err(UssError::DynamicError(format!(
                "File {} already exists",
                file_name
            )));
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_name)
            .map_err(to_error)?;

        file.set_len(file_size.into()).map_err(to_error)?;

        let index_mod = sieve::get_le_prime((file_size >> 10) as u32);

        // 1 (header size) + ceil(index_mod / (256 / 4))
        let data_offset = 2 + (index_mod - 1) >> 6;

        // in 256-byte chunks
        let data_size = (file_size >> 8) as u32 - data_offset;

        let header = DataLakeHeader {
            magic: b"DataLake".to_owned(),
            file_size,
            data_size,
            data_offset,
            data_next: data_offset,
            index_mod,
            index_offset: 1,
            index_offset_u32: 1 << 6,
        };

        let header_ptr = &header as *const DataLakeHeader;
        let header_u8ptr = header_ptr as *const u8;
        let header_size = std::mem::size_of::<DataLakeHeader>();
        let header_slice = unsafe { std::slice::from_raw_parts(header_u8ptr, header_size) };

        file.write_all(header_slice).map_err(to_error)?;

        return DataLake::load(file_name, false);
    }

    pub fn get_index_offset(&self, hash: &[u8; 50]) -> u32 {
        let checksum = crate::hasher::checksum_u32(hash, 50);

        return checksum % self.header.index_mod + self.header.index_offset_u32;
    }

    pub fn get(&mut self, hash: &[u8; 50]) -> Option<DataChunk> {
        match self.chunks.get(hash) {
            Some(val) => return Some(val.clone()),
            None => {
                let mut index_offset = self.get_index_offset(hash);

                loop {
                    let chunk_offset = self.data.read_u32(index_offset);

                    if chunk_offset == 0 {
                        return None;
                    }

                    let chunk = DataChunk::at(self.data.clone(), chunk_offset).ok()?;

                    if &chunk.header.hash != hash {
                        index_offset += 1;

                        continue;
                    }

                    self.chunks.insert(hash.to_owned(), chunk.clone());

                    return Some(chunk);
                }
            }
        }
    }
}
