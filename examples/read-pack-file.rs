use std::{path::PathBuf, io};
use git_walker::{ioerr, object_database, ioerre};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT}};

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let packfile_path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to a pack file"))?;
    let default_start_index = format!("{}", DATA_STARTS_AT);
    let search_at_index = match args.get(2) {
        Some(s) => s,
        None => &default_start_index,
    };
    let search_at_index = search_at_index.parse::<usize>().unwrap_or(DATA_STARTS_AT);
    let packfile_path = PathBuf::from(packfile_path);
    if !packfile_path.is_file() {
        return ioerre!("{:?} is not a file", packfile_path);
    }

    let right_now = std::time::Instant::now();
    let packfile = open_pack_file_ex(&packfile_path)?;
    println!("Packfile has {} objects", packfile.num_objects);
    println!("Looking for object at index {}", search_at_index);
    let (
        obj_type,
        length,
        object_starts_at,
    ) = packfile.get_object_type_and_len_at_index(search_at_index)?;
    println!("Found a {:?} object of size {}", obj_type, length);
    println!("compressed data starts at index {}", object_starts_at);

    let usize_len = length as usize;
    let decompressed_data = packfile.get_decompressed_data_from_index(usize_len, object_starts_at)?;
    println!("Got decompressed data successfully");

    match obj_type {
        object_database::packed::PackFileObjectType::Commit => {
            let commit_obj = CommitOnlyTreeAndParents::parse(&decompressed_data[..])?;
            println!("{:#?}", commit_obj);
        }
        object_database::packed::PackFileObjectType::Tree => {
            let tree_obj = TreeObject::parse(&decompressed_data[..])?;
            println!("{:#?}", tree_obj);
        }

        // dont care about these for now...
        object_database::packed::PackFileObjectType::Blob => {}
        object_database::packed::PackFileObjectType::Tag => {}
        object_database::packed::PackFileObjectType::OfsDelta(_) => {}
        object_database::packed::PackFileObjectType::RefDelta(_) => {}
    }

    println!("Ran command in {}ms", right_now.elapsed().as_millis());
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
