use std::{fs::DirEntry, path::{Path}, io};
use crate::fs_helpers;

mod index;
use index as index_file;
pub use index_file::*;

mod pack;
use pack as pack_file;
pub use pack_file::*;


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
        let mut index = search_packs_path.clone();
        let mut idx_file_name = prefix;
        idx_file_name.push_str(".idx");
        index.push(idx_file_name);
        out.push(PartiallyResolvedPackAndIndex::Unresolved(index));
    }

    Ok(out)
}

/// This resolved the index file for each `PartiallyResolvedPackAndIndex`
/// It does not resolve the actual pack file, because most of the time
/// we don't need to read every single pack file.
/// Returns an error if a single index file failed to be opened/read.
pub fn resolve_all_packs(
    packs: &mut Vec<PartiallyResolvedPackAndIndex>
) -> io::Result<()> {
    for pack in packs.iter_mut() {
        match pack {
            PartiallyResolvedPackAndIndex::Unresolved(path) => {
                let idx = open_idx_file(path)?;
                *pack = PartiallyResolvedPackAndIndex::IndexResolved(idx);
            }
            // no need to do anything if its already resolved:
            PartiallyResolvedPackAndIndex::IndexResolved(_) => {}
        }
    }
    Ok(())
}
