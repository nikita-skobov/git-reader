use std::{path::PathBuf, io, collections::HashMap};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid, Oid, full_oid_to_u128_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT, open_idx_file}};
use object_database::loose::UnparsedObjectType;

/// Like git-verify-pack but the difference is we don't calculate
/// a depth of delta objects...

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let packfilepath = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to the .pack file to verify"))?;

    let packfile_path = PathBuf::from(packfilepath);
    let idxfile_path = packfile_path.with_extension("idx");
    if !packfile_path.is_file() {
        return ioerre!("{:?} is not a file", packfile_path);
    }
    if ! idxfile_path.is_file() {
        return ioerre!("{:?} is not a file", idxfile_path);
    }

    let mut idx = open_idx_file(idxfile_path)?;
    idx.load_pack()?;

    let pack_map = idx.get_index_oid_map()?;
    let pack = match idx.pack {
        object_database::packed::PartiallyResolvedPackFile::Resolved(p) => p,
        object_database::packed::PartiallyResolvedPackFile::Unresolved(_) => {
            return ioerre!("idx pack is unresolved?");
        }
    };

    let mut iter = pack_map.iter();

    let mut ofs_delta_map: HashMap<usize, (String, Oid)> = HashMap::new();
    let mut ref_delta_map: HashMap<Oid, String> = HashMap::new();
    let (mut current_index, mut current_oid) = iter.next()
        .ok_or_else(|| ioerr!("Failed to read first oid/index of idx reverse map"))?;
    loop {
        let (
            obj_type,
            obj_size,
            _obj_starts_at
        ) = pack.get_object_type_and_len_at_index(*current_index)?;

        let mut extra_str = "".into();
        let obj_type_str = match obj_type {
            object_database::packed::PackFileObjectType::Commit => "commit",
            object_database::packed::PackFileObjectType::Tree => "tree",
            object_database::packed::PackFileObjectType::Blob => "blob",
            object_database::packed::PackFileObjectType::Tag => "tag",
            object_database::packed::PackFileObjectType::OfsDelta(ofs_index) => {
                let (base_type_str, base_oid) = match ofs_delta_map.get(&ofs_index) {
                    Some(i) => i,
                    None => {
                        return ioerre!("Failed to find a base object for an offset delta obj at index {}, trying to find index {}", current_index, ofs_index);
                    }
                };
                extra_str = format!("? {}", hex_u128_to_str(*base_oid));
                base_type_str
            },
            object_database::packed::PackFileObjectType::RefDelta(base_oid_full) => {
                let base_oid = full_oid_to_u128_oid(base_oid_full);
                let base_type_str = match ref_delta_map.get(&base_oid) {
                    Some(i) => i,
                    None => {
                        return ioerre!("Failed to find a base object for an ref delta obj at index {}, trying to find oid {}", current_index, hex_u128_to_str(base_oid));
                    }
                };
                extra_str = format!("? {}", hex_u128_to_str(base_oid));
                base_type_str
            }
        };
        
        let obj_type_str: String = obj_type_str.to_owned();
        ofs_delta_map.insert(*current_index, (obj_type_str.clone(), *current_oid));
        ref_delta_map.insert(*current_oid, obj_type_str.clone());
        
        let current_oid_str = hex_u128_to_str(*current_oid);
        match iter.next() {
            Some((next_index, next_oid)) => {
                // we calculate the size in packfile of this item
                // using the next index:
                let size_in_packfile = next_index - current_index;
                println!("{} {}\t{} {} {} {}", current_oid_str, obj_type_str, obj_size, size_in_packfile, current_index, extra_str);
                current_index = next_index;
                current_oid = next_oid;
            }
            None => {
                // to know how big the last object is,
                // we need to get the size of the packfile, and subtract
                // 20 for the trailer, and then we find the last index:
                let last_obj_index = pack.get_pack_size() - 20;
                let size_in_packfile = last_obj_index - current_index;
                println!("{} {}\t{} {} {} {}", current_oid_str, obj_type_str, obj_size, size_in_packfile, current_index, extra_str);
                break;
            }
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
