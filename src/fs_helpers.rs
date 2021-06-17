use std::{path::Path, fs, io};
use fs::DirEntry;

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
