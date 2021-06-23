use std::{path::PathBuf, io};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT, open_idx_file_light, IDXVersion}};
use object_database::loose::UnparsedObjectType;

/// like git-show-index but the index file is read from the cli
/// args, not stdin.

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let idxpath = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to the index file"))?;

    let idx = open_idx_file_light(&idxpath)?;
    let should_get_crc = idx.version == IDXVersion::V2;
    idx.walk_all_oids_with_index_and_from(None, |oid, fanout_index| {
        let crc_opt = if should_get_crc {
            idx.get_crc32_from_fanout_index_unchecked(fanout_index)
        } else {
            None
        };
        let pack_offset = idx.find_packfile_index_from_fanout_index(fanout_index);

        match (crc_opt, pack_offset) {
            (Some(crc), Some(offset)) => {
                println!("{} {:032x} ({:08x})", offset, oid, crc);
            },
            (None, Some(offset)) => {
                println!("{} {:032x}", offset, oid);
            },
            (_, None) => {
                println!("ERROR {:032x}", oid);
            }
        }

        false
    });
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
