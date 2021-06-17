use crate::object_id::*;
use crate::fs_helpers;
use std::path::{Path, PathBuf};
use std::{io, collections::HashMap, fs::DirEntry};

/// A loose object is either unresolved, in which case
/// it points to a file: 00/xyzdadadebebe that contains
/// the actual object, and we can read that file, and then
/// turn this into a resolved loose object, which has
/// the data loaded into memory.
#[derive(Debug)]
pub enum PartiallyResolvedLooseObject {
    Unresolved(PathBuf),
    Resolved(Vec<u8>),
}

/// git objects directory can have many loose
/// objects, where the first 2 characters of the sha hash
/// are the name of the folder, and then within that folder
/// are files that are the remainder of that sha hash.
/// This partially resolved loose map contains
/// a hash map of each of those sha hashes (first 2 chars of
/// folder name combined with file names within that folder),
/// and the value is an enum that is either the full object file
/// read into memory, or the path of that file that is ready to be
/// read.
#[derive(Debug)]
pub struct PartiallyResolvedLooseMap {
    pub map: HashMap<Oid, PartiallyResolvedLooseObject>,
}

impl PartiallyResolvedLooseMap {
    /// the given path should be the absolute path to the folder that contains
    /// all of the loose object folders, ie: /.../.git/objects/
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<PartiallyResolvedLooseMap> {
        let entries = fs_helpers::search_folder(path, filter_to_object_folder)?;
        let mut map = HashMap::new();
        for e in entries {
            for (hash, filepath) in e {
                map.insert(hash, PartiallyResolvedLooseObject::Unresolved(filepath));
            }
        }
        Ok(PartiallyResolvedLooseMap { map })
    }
}


#[inline(always)]
pub fn filter_to_object_folder(
    direntry: &DirEntry
) -> Option<Vec<(Oid, PathBuf)>> {
    let ftype = direntry.file_type().ok()?;
    if !ftype.is_dir() {
        return None;
    }
    let dname = direntry.file_name();
    let dname_str = dname.to_str()?;
    if dname_str.len() != 2 {
        return None;
    }
    // now we know this is an object folder,
    // so lets search through it, find all of the object files
    // and return a vec of map entries that should be
    // filled in later:
    let mut map_entries = vec![];
    let dirpath = direntry.path();
    let _ = fs_helpers::search_folder(dirpath, |objfile| -> Option<()> {
        let objfiletype = if let Ok(t) = objfile.file_type() {
            t
        } else {
            return None;
        };

        if !objfiletype.is_file() {
            return None;
        }

        let objfilename = objfile.file_name();
        let objfilename = if let Some(s) = objfilename.to_str() {
            s
        } else {
            return None;
        };

        // if this is a valid obj file name, it should be 38 hex chars
        if objfilename.len() != 38 {
            return None;
        }

        // now, we know this is a file we want, so lets
        // parse its file name/folder name into its u128 hash,
        // and also enter it into our map entries
        let hash = if let Ok(h) = hash_object_file_and_folder(dname_str, objfilename) {
            h
        } else {
            return None;
        };
        map_entries.push((hash, objfile.path()));

        // we dont need to return/collect anything here,
        // because we are appending our mutable map entries above
        None
    });
    return Some(map_entries);
}
