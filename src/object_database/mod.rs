use std::{path::{Path, PathBuf}, collections::HashMap, fs::DirEntry, io};
use crate::object_id::*;
use crate::{ioerr, fs_helpers};

mod loose;
use loose::*;

mod packed;
use packed::*;


#[derive(Debug)]
pub struct ObjectDB {
    loose: PartiallyResolvedLooseMap,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    packs: Vec<PartiallyResolvedPackAndIndex>,
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
