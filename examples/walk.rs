use git_walker::{object_id::{hash_str_to_oid, hex_u128_to_str, Oid, PartialOid}, object_database, ioerr, ioerre};
use object_database::loose::commit_object_parsing::CommitOnlyTreeAndParents;
use object_database::{loose::ParsedObject};
use std::io;

/// walk a commit and find all of its blobs

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let first = match args.get(1) {
        Some(f) => f,
        None => {
            eprintln!("Must provide a path to the .git/objects/ directory");
            std::process::exit(1);
        }
    };
    let commit = match args.get(2) {
        Some(c) => c,
        None => {
            eprintln!("Must provide a commit hash to start walking from");
            std::process::exit(1);
        }
    };

    todo!()
}
