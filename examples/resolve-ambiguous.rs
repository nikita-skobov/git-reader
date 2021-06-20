use std::io;
use git_walker::{ioerr, object_database::UnparsedObjectDB, object_id::{hex_u128_to_str, PartialOid}};
use git_walker::object_database::PartialSearchResult;

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to git objects db"))?;
    let ambiguous_oid = args.get(2)
        .ok_or_else(|| ioerr!("Must provide an OID to search"))?;

    let mut odb = UnparsedObjectDB::new(path)?;
    // need to resolve/load the .idx files in order
    // to search all oids
    odb.resolve_all_index_files()?;
    if ambiguous_oid.len() >= 32 {
        // we cannot resolve anything more than 32 bytes
        // because we only store/parse Oids up to 32 hex chars
        // ie: 16 bytes. This is as unambiguous as it gets.
        println!("{}", ambiguous_oid);
        return Ok(());
    }

    let partial_oid =  PartialOid::from_hash(ambiguous_oid)?;
    let result = odb.try_find_match_from_partial(partial_oid);

    match result {
        PartialSearchResult::FoundMatch(exact) => {
            println!("{}", exact);
        }
        PartialSearchResult::FoundMultiple(matches) => {
            eprintln!("{} is too ambiguous. Found matches:", ambiguous_oid);
            for id in matches {
                let id_str = hex_u128_to_str(id);
                eprintln!("{}", id_str);
            }
        }
        PartialSearchResult::FoundNone => {
            eprintln!("Failed to find any matches for {}", ambiguous_oid);
        }
    }


    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
