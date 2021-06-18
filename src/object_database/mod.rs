use std::{path::Path, io};

pub mod commit_object_parsing;

pub mod objects;
use objects::*;

pub mod loose;
use loose::*;

pub mod packed;
use packed::*;
use crate::object_id::Oid;


#[derive(Debug)]
pub struct ObjectDB {
    pub loose: PartiallyResolvedLooseMap,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    pub packs: Vec<PartiallyResolvedPackAndIndex>,
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

    pub fn find_object(&mut self, oid: Oid) {
        // first search if this oid is in the loose objects map
        if self.loose.contains_oid(oid) {
            // return/resolve this object id
            // either from memory, or from the file
        }
    }
}
