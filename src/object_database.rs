use std::{path::{Path, PathBuf}, collections::HashMap, fs::DirEntry, io};
use crate::object_id::*;
use crate::{ioerr, fs_helpers};


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


#[inline(always)]
pub fn filter_to_object_folder(
    direntry: &DirEntry
) -> Option<Vec<(u128, PathBuf)>> {
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
    let _ = fs_helpers::search_folder(dirpath, |objfile| -> Option<bool> {
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

#[derive(Debug)]
pub struct ObjectDB {
    loose: PartiallyResolvedLooseMap,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    packs: Vec<PartiallyResolvedPackAndIndex>,
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

impl ObjectDB {
    /// path should be the absolute path to the objects folder
    /// ie: /.../.git/objects/
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<ObjectDB> {
        let canon_path = path.as_ref().to_path_buf().canonicalize()?;
        let odb = ObjectDB {
            loose: PartiallyResolvedLooseMap::from_path(&canon_path)?,
            packs: get_vec_of_unresolved_packs(&canon_path)?,
        };
        Ok(odb)
    }
}
