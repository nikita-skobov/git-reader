use std::path::Path;
use crate::{object_id::{full_oid_from_str, OidFull}};

mod index;
use index as index_file;
pub use index_file::*;

mod pack;
use pack as pack_file;
pub use pack_file::*;

pub mod delta;
pub use delta::*;

pub fn parse_pack_or_idx_id<P: AsRef<Path>>(
    path: P
) -> Option<OidFull> {
    let path = path.as_ref();
    let file_name = path.file_name()?;
    let file_name = file_name.to_str()?;
    // the 40 hex char hash should be
    // between the 5th and 45th character:
    // pack-{40 hex chars}.idx (or .pack)
    let file_hash = file_name.get(5..45)?;
    let file_id = full_oid_from_str(file_hash)?;
    Some(file_id)
}
