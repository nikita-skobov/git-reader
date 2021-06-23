use std::{path::PathBuf, io, collections::HashMap};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid, Oid, full_oid_to_u128_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT}};
use object_database::loose::UnparsedObjectType;

/// Like git-verify-pack but the difference is we don't calculate
/// a depth of delta objects...

pub fn realmain() -> io::Result<()> {
    todo!()
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
