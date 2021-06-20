use std::{path::PathBuf, io};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT, open_idx_file}};
use object_database::loose::UnparsedObjectType;

/// This program is similar to git-show-index
/// except it will not show the CRC32 of the objects

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let idxpath = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to the index file"))?;

    let idx = open_idx_file(idxpath)?;

    idx.walk_all_oids(|oid| {
        let oid_str = hex_u128_to_str(oid);
        match idx.get_packfile_index_of_oid(oid) {
            Ok(Some(i)) => {
                println!("{} {}", i, oid_str);
            }
            _ => {
                println!("ERROR {}", oid_str);
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
