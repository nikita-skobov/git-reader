use crate::object_id::*;
use crate::{fs_helpers, ioerre, ioerr};
use std::path::{Path, PathBuf};
use std::{io, collections::HashMap, fs::DirEntry};

pub mod parsed;
pub use parsed::*;

pub mod unparsed;
pub use unparsed::*;

pub trait Resolve {
    type Object;
    fn unresolved(p: PathBuf) -> Self;
    fn resolve_or_return(&mut self) -> io::Result<Option<&Self::Object>>;
    fn return_if_resolved(&self) -> io::Result<Option<&Self::Object>>;
}

/// git objects directory can have many loose
/// objects, where the first 2 characters of the sha hash
/// are the name of the folder, and then within that folder
/// are files that are the remainder of that sha hash.
/// This partially resolved loose map contains
/// a hash map of each of those sha hashes (first 2 chars of
/// folder name combined with file names within that folder),
/// and the value is an enum that is either the pathbuf to that file
/// or a resolved and potentially parsed object. type parameter T
/// determined what gets stored here. If you use:
/// `T = PartiallyResolvedLooseObject` then we store the raw data
/// without parsing, but if you use: `T = PartiallyParsedLooseObject`
/// then when we resolve the object file, we parse it fully and store
/// the parsed object. The parsing for commit objects can be further
/// fine tuned in the type parameter of the `PartiallyParsedLooseObject`
/// enum, ie: you can parse a commit fully, or just parse the tree/parents,
/// etc..
pub struct PartiallyResolvedLooseMap<T: Resolve> {
    pub map: HashMap<Oid, T>,
}

impl<T: Resolve> PartiallyResolvedLooseMap<T> {
    /// the given path should be the absolute path to the folder that contains
    /// all of the loose object folders, ie: /.../.git/objects/
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let entries = fs_helpers::search_folder(path, filter_to_object_folder)?;
        let mut map = HashMap::new();
        for e in entries {
            for (hash, filepath) in e {
                let unresolved = T::unresolved(filepath);
                map.insert(hash, unresolved);
            }
        }
        Ok(Self { map })
    }

    pub fn contains_oid(&self, oid: Oid) -> bool {
        self.map.contains_key(&oid)
    }

    /// pass in a hex string and we will convert it
    /// to a 128bit Oid for you.
    /// hash must be at least 32 chars.
    pub fn contains_hash(&self, hash: &str) -> io::Result<bool> {
        let oid = hash_str_to_oid(hash)?;
        Ok(self.map.contains_key(&oid))
    }

    /// needs to be mutable in case the desired object exists in
    /// the map, but is not resolved yet, so we need to resolve it.
    /// returns an error if there was an error during the resolving process.
    /// inside error is Option which is None if the desired object id does
    /// not exist
    pub fn get_object<'a>(&'a mut self, oid: Oid) -> io::Result<Option<&'a T::Object>> {
        match self.map.get_mut(&oid) {
            None => Ok(None),
            Some(partially_resolved) => partially_resolved.resolve_or_return()
        }
    }

    pub fn get_object_existing<'a>(&'a self, oid: Oid) -> io::Result<Option<&'a T::Object>> {
        match self.map.get(&oid) {
            None => Ok(None),
            Some(partially_resolved) => partially_resolved.return_if_resolved()
        }
    }

    /// iterate all objects in map, and try to resolve each one.
    /// returns an error if any of the resolutions fails.
    pub fn resolve_all(&mut self) -> io::Result<()> {
        let all_keys: Vec<Oid> = self.map.keys().map(|k| *k).collect();
        for key in all_keys {
            self.get_object(key)?;
        }
        Ok(())
    }

    /// iterate all Oid keys and return which key matches
    /// your partial id. returns None if no match found
    pub fn try_find_match_from_partial(&self, partial_oid: Oid) -> Option<Oid> {
        for key in self.map.keys() {
            let key = *key;
            if key & partial_oid == partial_oid {
                return Some(key);
            }
        }

        None
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
