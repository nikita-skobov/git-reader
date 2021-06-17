use std::{fs::DirEntry, path::{Path, PathBuf}, io};
use crate::fs_helpers;

#[derive(Debug)]
pub enum PartiallyResolvedPackAndIndex {
    /// pointer to index, and pack file respectively
    Unresolved(PackAndIndex),

    /// The index file is resolved, an in memory,
    /// but the pack file is still just the path to the file
    IndexResolved(Vec<u8>, PathBuf),

    /// both are resolved and in memory:
    BothResolved(Vec<u8>, Vec<u8>),
}

/// pointer to the index (*.idx) and pack (*.pack) files
#[derive(Debug)]
pub struct PackAndIndex {
    pub pack: PathBuf,
    pub index: PathBuf,
}

#[inline(always)]
pub fn get_pack_file_prefix_string(direntry: &DirEntry) -> Option<String> {
    let fileobj = direntry.path();
    if !fileobj.is_file() {
        return None;
    }

    let filename = fileobj.file_name()?.to_str()?;
    if !filename.starts_with("pack") || !filename.ends_with(".idx") {
        return None;
    }

    // TODO: is it safe to assume that
    // pack files will always be this length?
    // pack-{40 hex chars}.idx -> we want first 45 chars:
    match filename.get(0..45) {
        Some(s) => Some(s.to_string()),
        None => None
    }
}

/// path should be the absolute path to /.../.git/objects/
pub fn get_vec_of_unresolved_packs<P: AsRef<Path>>(
    path: P
) -> io::Result<Vec<PartiallyResolvedPackAndIndex>> {
    let mut out = vec![];
    let mut search_packs_path = path.as_ref().to_path_buf();
    search_packs_path.push("pack");

    let prefixes = fs_helpers::search_folder(
        &search_packs_path, get_pack_file_prefix_string)?;

    // now we have a vec of prefixes, where each prefix
    // is "pack-{40 hex chars}"
    // so we want to convert that to pathbufs and add the .idx, and .pack
    // extensions
    for prefix in prefixes {
        let mut pack = search_packs_path.clone();
        let mut index = search_packs_path.clone();
        let mut pack_file_name = prefix.clone();
        let mut idx_file_name = prefix;
        pack_file_name.push_str(".pack");
        idx_file_name.push_str(".idx");
        pack.push(pack_file_name);
        index.push(idx_file_name);
        let pack_and_index = PackAndIndex {
            pack,
            index,
        };
        out.push(PartiallyResolvedPackAndIndex::Unresolved(pack_and_index));
    }

    Ok(out)
}
