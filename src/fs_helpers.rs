use std::{path::Path, fs, io};
use fs::{OpenOptions, DirEntry, File};
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

/// an alternative to `search_folder`.
/// this returns as soon as your callback returns an error.
pub fn search_folder_out<P, F>(
    path: P,
    should_use_entry: F
) -> io::Result<()> where
    P: AsRef<Path>,
    F: FnMut(&DirEntry) -> io::Result<()>
{
    let mut should_use_entry = should_use_entry;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if let Err(e) = should_use_entry(&entry) {
            return Err(e);
        }
    }
    Ok(())
}

/// an alternative to `search_folder_out` where
/// we treat the search folder as missing as being ok.
/// this returns as soon as your callback returns an error.
pub fn search_folder_out_missing_ok<P, F>(
    path: P,
    should_use_entry: F
) -> io::Result<()> where
    P: AsRef<Path>,
    F: FnMut(&DirEntry) -> io::Result<()>
{
    let mut should_use_entry = should_use_entry;
    let readdir_call = fs::read_dir(path);
    let readdir = match readdir_call {
        Ok(r) => Ok(r),
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => {
                    // if a folder is not found, thats ok
                    return Ok(());
                }
                _ => Err(e),
            }
        }
    };
    for entry in readdir? {
        let entry = entry?;
        if let Err(e) = should_use_entry(&entry) {
            return Err(e);
        }
    }
    Ok(())
}

pub fn get_mmapped_file<P: AsRef<Path>>(
    path: P,
) -> io::Result<Mmap> {
    let file = OpenOptions::new().read(true)
        .write(false).create(false).open(path)?;
    let mmapped_file = unsafe { MmapOptions::new().map(&file)? };
    Ok(mmapped_file)
}

pub fn get_readonly_handle<P: AsRef<Path>>(
    path: P
) -> io::Result<File> {
    let file = OpenOptions::new().read(true)
        .write(false).create(false).open(path)?;
    Ok(file)
}
