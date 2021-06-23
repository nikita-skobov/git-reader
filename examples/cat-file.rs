use std::{path::PathBuf, io, collections::{BTreeMap, BTreeSet}, time::Instant};
use git_walker::{ioerr, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid, Oid}};
use git_walker::{printoid, object_database::{LightObjectDB, FoundObjectLocation, loose::{commit_object_parsing::CommitFull, ParsedObject, UnparsedObject, ParseEverythingBlobStringsLossy}}, eprintoid, ioerre};

/// Like git-cat-file, but it defaults to "-p", ie: it just
/// prints the contents of the object found via its OID.

pub fn disambiguate(
    ambiguous_oid: &String,
    odb: &LightObjectDB,
) -> io::Result<(Oid, FoundObjectLocation)> {
    let partial_oid =  PartialOid::from_hash(ambiguous_oid)?;
    let mut found_set = BTreeMap::new();
    odb.find_matching_oids_with_locations(partial_oid, |oid, location| {
        found_set.insert(oid, location);
    })?;

    let found_len = found_set.len();
    if found_len == 1 {
        let (oid, _) = found_set.iter().next().unwrap();
        let oid = *oid;
        let location = found_set.remove(&oid).unwrap();
        return Ok((oid, location))
    }

    if found_len == 0 {
        return ioerre!("Failed to find object matching {}", ambiguous_oid);
    } else {
        let mut err_str = format!("Error: '{}' is too ambiguous", ambiguous_oid);
        err_str = format!("{}\nhint: The candidates are:", err_str);
        for (found_oid, _) in found_set.iter() {
            err_str = format!("{}\n{:032x}", err_str, found_oid);
        }
        return ioerre!("{}", err_str);
    }
}

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to git objects db"))?;
    let ambiguous_oid = args.get(2)
        .ok_or_else(|| ioerr!("Must provide an OID to search"))?;

    // let now = Instant::now();
    let odb = LightObjectDB::new(&path)?;
    let (oid, location) = if ambiguous_oid.len() < 32 {
        // if its not a full oid, we need
        // to disambiguate, so traverse everything,
        // and find all matches:
        let (oid, location) = disambiguate(ambiguous_oid, &odb)?;
        (oid, Some(location))
    } else {
        // if its already 32 hex chars or longer,
        // we can just make it into an Oid:
        (hash_str_to_oid(ambiguous_oid)?, None)
    };

    // if we disambiguated, we have the location, but if we
    // just had a full object passed to us, we need to find its location:
    let location = match location {
        Some(l) => l,
        None => {
            // find where the resolved oid is:
            let (_, l) = odb.find_first_matching_oid_with_location(oid)?;
            l
        }
    };

    let object: ParsedObject<ParseEverythingBlobStringsLossy> = odb.get_object_from_location(location)?;
    // let object: UnparsedObject = odb.get_object_from_location(location)?;
    println!("{}", object);
    // println!("Elapsed: {}us", now.elapsed().as_micros());
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
