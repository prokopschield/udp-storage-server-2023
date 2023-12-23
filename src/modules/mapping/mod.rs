use super::error::{to_error, UssResult};

pub struct MemoryMapping {
    owned_ro: Option<std::sync::Arc<std::sync::Mutex<memmap::Mmap>>>,
    owned_rw: Option<std::sync::Arc<std::sync::Mutex<memmap::MmapMut>>>,
    roref: &'static [u8],
}

pub fn create_ro_mapping(file_path: &str) -> UssResult<MemoryMapping> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(file_path)
        .map_err(to_error)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file).map_err(to_error)? };

    let slice = &mmap[..];
    let arc = std::sync::Arc::from(std::sync::Mutex::from(mmap));

    return Ok(MemoryMapping {
        owned_ro: Some(arc),
        owned_rw: None,
        roref: slice,
    });
}

pub fn create_rw_mapping(file_path: &str) -> UssResult<MemoryMapping> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)
        .map_err(to_error)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file).map_err(to_error)? };

    let mmut = mmap.make_mut().map_err(to_error)?;

    let slice = &mmap[..];
    let mutex = std::sync::Mutex::from(mmut);
    let arc = std::sync::Arc::from(mutex);

    Ok(MemoryMapping {
        owned_ro: None,
        owned_rw: Some(arc),
        roref: slice,
    })
}
