use std::{path::Path, fs, io};
use fs::{OpenOptions, DirEntry};
use memmap2::{Mmap, MmapOptions};

pub fn search_folder<P, F, T>(
    path: P,
    should_use_entry: F,
) -> io::Result<Vec<T>> where
    P: AsRef<Path>,
    F: FnMut(&DirEntry) -> Option<T>,
{
    let mut should_use_entry = should_use_entry;
    let mut out = vec![];
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let use_entry = should_use_entry(&entry);
        if let Some(t) = use_entry {
            out.push(t);
        }
    }
    Ok(out)
}


pub fn get_mmapped_file<P: AsRef<Path>>(
    path: P,
) -> io::Result<Mmap> {
    let file = OpenOptions::new().read(true)
        .write(false).create(false).open(path)?;
    let mmapped_file = unsafe { MmapOptions::new().map(&file)? };
    Ok(mmapped_file)
}
