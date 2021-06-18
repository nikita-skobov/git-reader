use std::{path::Path, io};
use crate::object_id::Oid;

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

    pub fn find_object(&mut self, oid: Oid) {
        // first search if this oid is in the loose objects map
        if self.loose.contains_oid(oid) {
            // return/resolve this object id
            // either from memory, or from the file
        }
    }
}
