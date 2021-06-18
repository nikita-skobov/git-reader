use std::{path::Path, io};
use crate::{ioerre, object_id::{oid_full_to_string, Oid}, ioerr};

pub mod loose;
use loose::*;

pub mod packed;
use packed::*;

/// A type alias for an ObjectDB that stores raw
/// data when resolved. ie: the data it stores
/// is the decompressed zlib stream that is in each
/// loose object file.
pub type UnparsedObjectDB = ObjectDB<PartiallyResolvedLooseObject>;

/// A type alias for an ObjectDB that stores actual parsed data
/// when resolved. Further refined by specifying a desired
/// type for T which determines how commits are parsed, ie: which
/// information is desired to be parsed from commits, and the rest is
/// not parsed. This type parameter does not apply to
/// trees, blobs, or tags because those do not
/// have alternate ways to parse them. trees need to be fully parsed
/// otherwise they are useless. Blobs and tags currently are not parsed
pub type ParsedObjectDB<T> = ObjectDB<PartiallyParsedLooseObject<T>>;

pub struct ObjectDB<T: Resolve> {
    pub loose: PartiallyResolvedLooseMap<T>,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    pub packs: Vec<PartiallyResolvedPackAndIndex>,
}

impl<T: Resolve> ObjectDB<T> {
    /// path should be the absolute path to the objects folder
    /// ie: /.../.git/objects/
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<ObjectDB<T>> {
        let canon_path = path.as_ref().to_path_buf().canonicalize()?;
        let odb = ObjectDB {
            loose: PartiallyResolvedLooseMap::from_path(&canon_path)?,
            packs: get_vec_of_unresolved_packs(&canon_path)?,
        };
        Ok(odb)
    }

    /// get the object if we already resolved it,
    /// and if not, we will resolve it which is why we need to be
    /// mutable
    pub fn get_object_mut<'a>(&'a mut self, oid: Oid) -> io::Result<&'a T::Object> {
        // first search if this oid is in the loose objects map
        let obj_in_loose = self.loose.get_object(oid)?;
        match obj_in_loose {
            Some(obj) => Ok(obj),
            None => {
                return ioerre!("Oid: {} not found. TODO: need to implement searching through pack file", oid);
            }
        }
    }

    /// get an object if it exists. We cannot resolve here
    /// because we are not mutable, so objects being not resolved
    /// is the same as them not existing... only use this
    /// if you resolved all objects ahead of time.
    pub fn get_object<'a>(&'a self, oid: Oid) -> io::Result<&'a T::Object> {
        // first search if this oid is in the loose objects map
        let obj_in_loose = self.loose.get_object_existing(oid)?;
        if let Some(obj) = obj_in_loose {
            return Ok(obj);
        }

        // we failed to find it in the loose map, so now
        // we search through all of our index files to see if
        // it exists in one of them.
        // Note that this will fail to find it if we have not resolved
        // the index files beforehand.
        // If you want to optionally resolve the index files as we go, use
        // `get_object_mut` instead.
        for idx_pack in self.packs.iter() {
            let idx = match idx_pack {
                PartiallyResolvedPackAndIndex::Unresolved(_) => {
                    // if its not resolved, skip this one
                    // because we cannot resolve it anyway
                    continue;
                }
                PartiallyResolvedPackAndIndex::IndexResolved(idx) => idx,
            };
            let has_index = idx.find_index_for(oid)?;
            if let Some(index) = has_index {
                let idx_id_str = oid_full_to_string(idx.id)?;
                eprintln!("Found {:0x} in {}", oid, idx_id_str);
                // TODO: what to do if we find it?
            }
        }

        return ioerre!("Oid: {} not found. TODO: need to implement searching through pack file", oid);
    }

    /// iterates the vec of partially resolved packs,
    /// and loads the .idx file/parses its header if it
    /// has not been resolved yet.
    pub fn resolve_all_index_files(&mut self) -> io::Result<()> {
        resolve_all_packs(&mut self.packs)
    }

    /// result a single .idx file at the given index in the packs vec.
    /// returns an error if either we failed to open/parse the .idx file
    /// or if the provided index is out of range
    pub fn resolve_index_file(&mut self, index: usize) -> io::Result<()> {
        let packs_len = self.packs.len();
        let pack = self.packs.get_mut(index)
            .ok_or_else(|| ioerr!("Packs vec has length of {}, but you provided an index of {}", packs_len, index))?;
        match pack {
            PartiallyResolvedPackAndIndex::Unresolved(path) => {
                let idx = open_idx_file(path)?;
                *pack = PartiallyResolvedPackAndIndex::IndexResolved(idx);
            }
            // it is already resolved, no need to do anything
            PartiallyResolvedPackAndIndex::IndexResolved(_) => {}
        }
        Ok(())
    }
}
