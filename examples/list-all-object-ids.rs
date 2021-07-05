use std::io;
use git_reader::{ioerr, object_id::{hex_u128_to_str}};

/// lists all object ids found in this object database,
/// both loose and packed objects.

pub fn realmain() -> io::Result<()> {
    todo!()
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
