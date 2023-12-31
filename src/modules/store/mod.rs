pub mod sieve;

use crate::compression::compressor::CompressorCollection;

use super::{compression::decompress, error::*, mapping::*};
use std::{collections::HashMap, io::Write, rc::Rc};

#[derive(Copy, Clone)]
#[repr(C)]
pub struct DataChunkHeader {
    pub hash: [u8; 50],
    pub uncompressed_length: u16,
    pub compressed_length: u16,
}

const HEADER_SIZE: usize = std::mem::size_of::<DataChunkHeader>();

#[derive(Clone)]
pub struct DataChunk {
    pub header: DataChunkHeader,
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
    // index_offset: *mut u32 = (hasher::checksum(hash) % index_mod) + index_offset_u32
    index_mod: u32,
    // max value of index_offset before overflow into data
    index_max: u32,
    // index begins here (in 256-byte chunks)
    index_offset: u32,
    // index_offset << 6
    index_offset_u32: u32,
}

pub struct DataLake {
    data: Rc<MemoryMapping>,
    chunks: HashMap<[u8; 50], DataChunk>,
    // if MemoryMapping is readonly, modifying header will lead to SIGSEGV
    header: &'static mut DataLakeHeader,
    readonly: bool,
    compressors: CompressorCollection,
}

impl DataLake {
    pub fn load(filename: &str, readonly: bool) -> UssResult<DataLake> {
        let data_map = if readonly {
            create_ro_mapping(filename)?
        } else {
            create_rw_mapping(filename)?
        };

        let header_ptr = data_map.roref.as_ptr() as *mut DataLakeHeader;

        let header: &'static mut DataLakeHeader = match unsafe { header_ptr.as_mut() } {
            Some(header) => header,
            None => {
                return Err(UssError::StaticError(
                    "DataLake::load: lake pointer is NULL",
                ))
            }
        };

        Ok(DataLake {
            data: Rc::from(data_map),
            chunks: HashMap::new(),
            header,
            readonly,
            compressors: CompressorCollection::new(),
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

        let index_offset = 1;
        let index_offset_u32 = 1 << 6;

        let index_mod = sieve::get_le_prime((file_size >> 10) as u32);
        let index_max = (index_offset + 1 + ((index_mod - 1) >> 6)) << 6;

        // 1 (header size) + ceil(index_mod / (256 / 4))
        let data_offset = 2 + ((index_mod - 1) >> 6);

        // in 256-byte chunks
        let data_size = (file_size >> 8) as u32 - data_offset;

        let header = DataLakeHeader {
            magic: b"DataLake".to_owned(),
            file_size,
            data_size,
            data_offset,
            data_next: data_offset,
            index_mod,
            index_max,
            index_offset,
            index_offset_u32,
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

    pub fn put(&mut self, data: &[u8]) -> UssResult<DataChunk> {
        let hash = super::hasher::hash(data);
        let existing = self.get(&hash);

        match existing {
            Some(chunk) => return Ok(chunk),
            None => (),
        };

        if self.readonly {
            return Err(UssError::StaticError(
                "Must not call put() on readonly lake.",
            ));
        }

        let mut map = match &self.data.owned_rw {
            Some(arc) => arc.lock().map_err(to_error)?,
            None => return Err(UssError::StaticError("put() called on read-only map")),
        };

        let compressed = self.compressors.compress(data)?;

        let uncompressed_length = data.len() as u16;
        let compressed_length = compressed.len() as u16;

        let header = DataChunkHeader {
            hash,
            uncompressed_length,
            compressed_length,
        };

        let offset = self.header.data_next;
        let offset_bytes = offset_to_data_offset(offset);
        let alloc_size: usize = HEADER_SIZE + compressed_length as usize;

        unsafe {
            // write header
            let write_location = &mut map[offset_bytes..offset_bytes + alloc_size];
            let header_ptr = write_location.as_mut_ptr() as *mut DataChunkHeader;

            header_ptr.copy_from(&header, 1);
        }

        let write_location = &mut map[offset_bytes + HEADER_SIZE..offset_bytes + alloc_size];

        write_location.copy_from_slice(&compressed);

        (*self.header).data_next += ((alloc_size - 1) >> 8) as u32 + 1;

        {
            // add to index
            let mut index_offset = self.get_index_offset(&hash);

            loop {
                if index_offset > self.header.index_max {
                    return Err(UssError::StaticError("DataLake index ran out of space."));
                }

                if self.data.read_u32(index_offset) != 0 {
                    index_offset += 1;
                    continue;
                }

                let map_offset = (index_offset as usize) << 2;

                map[map_offset..map_offset + 4].copy_from_slice(&offset.to_le_bytes());

                break;
            }
        }

        Ok(DataChunk {
            header,
            mapping: self.data.clone(),
            offset,
        })
    }
}
