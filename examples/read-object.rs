use std::{path::PathBuf, io};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents, CommitFull}}, packed::{open_pack_file_ex, DATA_STARTS_AT, open_idx_file}};
use object_database::loose::UnparsedObjectType;

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let dbpath = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to the .git/objects/ directory"))?;
    let oid_str = args.get(2)
        .ok_or_else(|| ioerr!("Must provide an oid to try to read"))?;

    let mut odb = object_database::ParsedObjectDB::<CommitFull>::new(dbpath)
        .map_err(|e| ioerr!("Failed to read/load object DB\n{}", e))?;

    odb.loose.resolve_all()?;
    odb.fully_resolve_all_packs()?;

    let use_oid = if oid_str.len() < 32 {
        let partial_oid = PartialOid::from_hash(oid_str)?;
        let oid = match odb.try_find_match_from_partial(partial_oid) {
            object_database::PartialSearchResult::FoundMatch(exact) => exact,
            object_database::PartialSearchResult::FoundMultiple(multiple) => {
                let mut err_str = "Ambiguous oid, matches multiple:\n".into();
                for m in multiple {
                    let m_str = hex_u128_to_str(m);
                    err_str = format!("{}{}\n", err_str, m_str);
                }
                return ioerre!("{}", err_str);
            }
            object_database::PartialSearchResult::FoundNone => {
                return ioerre!("No match found for partial oid: {}", hex_u128_to_str(partial_oid.oid));
            }
        };
        oid
    } else {
        hash_str_to_oid(oid_str)?
    };


    let parsed_obj = odb.get_object(use_oid)?;
    let parsed_obj = match parsed_obj {
        object_database::ReturnedObject::Borrowed(b) => b,
        object_database::ReturnedObject::Owned(ref o) => o,
    };

    print!("{}", parsed_obj);
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
