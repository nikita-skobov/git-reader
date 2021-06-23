use std::{path::PathBuf, io, collections::BTreeSet, time::Instant};
use git_walker::{ioerr, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid, Oid}};
use git_walker::{printoid, object_database::{LightObjectDB}, eprintoid};

/// given a path to the git objects db, and a partial OID, try
/// to resolve it to a single OID, or otherwise report if there
/// are other OIDs that are similar to it.

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to git objects db"))?;
    let ambiguous_oid = args.get(2)
        .ok_or_else(|| ioerr!("Must provide an OID to search"))?;

    let now = Instant::now();
    let odb = LightObjectDB::new(&path)?;
    let partial_oid =  PartialOid::from_hash(ambiguous_oid)?;
    let mut found_set = BTreeSet::new();
    odb.find_matching_oids(partial_oid, |oid| {
        found_set.insert(oid);
    })?;

    let found_len = found_set.len();
    if found_len == 0 {
        eprintln!("Failed to find object matching {}", ambiguous_oid);
    } else if found_len == 1 {
        let found = found_set.iter().next().unwrap();
        printoid!(found);
    } else {
        eprintln!("Error: '{}' is too ambiguous", ambiguous_oid);
        eprintln!("hint: The candidates are:");
        for found_oid in found_set.iter() {
            eprintoid!(found_oid);
        }
    }
    println!("Elapsed: {}us", now.elapsed().as_micros());
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
