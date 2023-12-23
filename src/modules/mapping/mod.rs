pub enum MemoryMapping {
    ReadOnly(std::sync::Arc<std::sync::Mutex<memmap::Mmap>>),
    ReadWrite(std::sync::Arc<std::sync::Mutex<memmap::MmapMut>>),
}

pub fn create_ro_mapping(file_path: &str) -> std::io::Result<MemoryMapping> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(file_path)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file)? };

    return Ok(MemoryMapping::ReadOnly(std::sync::Arc::from(
        std::sync::Mutex::from(mmap),
    )));
}

pub fn create_rw_mapping(file_path: &str) -> std::io::Result<MemoryMapping> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;

    let mmap = unsafe { memmap::MmapOptions::new().map(&file)? };

    let mmut = mmap.make_mut()?;

    return Ok(MemoryMapping::ReadWrite(std::sync::Arc::from(
        std::sync::Mutex::from(mmut),
    )));
}
