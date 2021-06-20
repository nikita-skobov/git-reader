use std::io;
use git_walker::{ioerr, object_database::UnparsedObjectDB, object_id::{hex_u128_to_str}};
use git_walker::object_database::PartialSearchResult;

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to git objects db"))?;

    let right_now = std::time::Instant::now();
    let mut odb = UnparsedObjectDB::new(path)?;
    // need to resolve/load the .idx files in order
    // to search all oids
    odb.resolve_all_index_files()?;
    odb.walk_all_oids(|oid| {
        println!("{}", hex_u128_to_str(oid));
    });

    println!("Ran command in {}ms", right_now.elapsed().as_millis());
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
