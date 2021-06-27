use std::{path::PathBuf, io, collections::{BTreeMap, HashMap}, convert::TryInto};
use git_walker::{ioerr, object_database, ioerre, object_id::{hex_u128_to_str, PartialOid, hash_str_to_oid, Oid, full_oid_to_u128_oid}};
use object_database::{loose::{tree_object_parsing::TreeObject, commit_object_parsing::{ParseCommit, CommitOnlyTreeAndParents}}, packed::{open_pack_file_ex, DATA_STARTS_AT, open_idx_file_light, IDXFileLight, PackFile}};
use object_database::{LightObjectDB, loose::UnparsedObjectType};

/// Like git-verify-pack but the difference is we don't calculate
/// a depth of delta objects...

pub const COMMIT: &'static str = "commit";
pub const TREE: &'static str = "tree";
pub const BLOB: &'static str = "blob";
pub const TAG: &'static str = "tag";

pub fn resolve_to_base_type(
    idxfile: &mut IDXFileLight,
    packfile: &PackFile,
    base_oid: Oid,
) -> io::Result<&'static str> {
    // for ref deltas, we actually arent guaranteed
    // that they are before this object, its possible
    // they come after. so we have to do something
    // inefficient here which is to read
    // the idx file again, find where in the packfile
    // this id exists in, and then find the object type
    // at that index:
    // eprintln!("Checking if index file has {:032x}", base_oid);
    let fanout_index = idxfile.find_oid_and_fanout_index(base_oid)
        .expect("Failed to find base oid in index file");
    let packfile_index_of_base = idxfile.find_packfile_index_from_fanout_index(fanout_index)
        .expect("Failed to find packfile index from idx fanout index");
    let packfile_index_of_base: usize = packfile_index_of_base.try_into()
        .expect("Failed to convert u64 to usize");
    // eprintln!("Found it at {}", packfile_index_of_base);
    let (
        base_obj_type,
        _,
        _,
    ) = packfile.get_object_type_and_len_at_index(packfile_index_of_base)?;
    let found = match base_obj_type {
        object_database::packed::PackFileObjectType::Commit => COMMIT,
        object_database::packed::PackFileObjectType::Tree => TREE,
        object_database::packed::PackFileObjectType::Blob => BLOB,
        object_database::packed::PackFileObjectType::Tag => TAG,
        object_database::packed::PackFileObjectType::OfsDelta(_) => {
            // not sure if a ref can point to a delta offset object?
            panic!("can a ref point to a delta?");
        },
        object_database::packed::PackFileObjectType::RefDelta(base_id) => {
            let base_oid = full_oid_to_u128_oid(base_id);
            resolve_to_base_type(idxfile, packfile, base_oid)?
        }
    };
    Ok(found)
}

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to a packfile"))?;
    let packfile_path = PathBuf::from(path);
    let idxfile_path = packfile_path.with_extension("idx");

    let packfile = open_pack_file_ex(&packfile_path)?;
    let mut idxfile = open_idx_file_light(&idxfile_path)?;
    // first, get the idx entry map.
    // we want to traverse the packed objects in order
    // they appear in the packfile, but in the idx file
    // they have a different order. a simple solution
    // is to store their indices in a BTreeMap so we can
    // then iterate in order:
    let mut idx_map = BTreeMap::new();
    idxfile.walk_all_oids_with_index_and_from(None, |oid, fanout_index| {
        // this should be safe to unwrap because we know this oid is
        // in the packfile... if this unwrap fails, we either have
        // an invalid packfile (which should have been caught when we opened it)
        // or otherwise our parsing/searching code is wrong...
        let packfile_index = idxfile.find_packfile_index_from_fanout_index(fanout_index)
            .unwrap();
        idx_map.insert(packfile_index as usize, oid);
        false
    });

    // this map stores packfile indices and maps to
    // the base object that it refers to. a delta ofset
    // always points to a past object. so if we
    // see a delta offset, we know we can lookup its
    // base in this map:
    let mut delta_map: BTreeMap<usize, (Oid, &'static str)> = BTreeMap::new();
    let mut ite = idx_map.iter().peekable();
    loop {
        let (packfile_index, oid) = match ite.next() {
            Some(po) => po,
            None => { break; }
        };
        let (
            obj_type,
            obj_decompressed_size,
            _,
        ) = packfile.get_object_type_and_len_at_index(*packfile_index)?;
        let next_index = match ite.peek() {
            Some((i, _)) => **i,
            None => {
                // if there is no next index, instead
                // we use the length of the file:
                packfile.get_pack_size() as usize
            }
        };
        let size_in_packfile = next_index - packfile_index;

        let (typestr, base_oid): (&'static str, Option<Oid>) = match obj_type {
            object_database::packed::PackFileObjectType::Commit => (COMMIT, None),
            object_database::packed::PackFileObjectType::Tree => (TREE, None),
            object_database::packed::PackFileObjectType::Blob => (BLOB, None),
            object_database::packed::PackFileObjectType::Tag => (TAG, None),
            object_database::packed::PackFileObjectType::OfsDelta(base_index) => {
                // we get this object via its base index.
                // again this would be an error if this object
                // did not exist...
                let (base_oid, base_type_str) = delta_map.get(&base_index)
                    .expect("delta offset object failed to find its base object. something is horribly wrong");
                (base_type_str, Some(*base_oid))
            }
            object_database::packed::PackFileObjectType::RefDelta(base_oid) => {
                let base_oid = full_oid_to_u128_oid(base_oid);
                let base_type_str = resolve_to_base_type(&mut idxfile, &packfile, base_oid)?;
                (base_type_str, Some(base_oid))
            }
        };
        delta_map.insert(*packfile_index, (*oid, typestr));

        if let Some(base_oid) = base_oid {
            println!("{:032x} {}\t{} {} {} ? {:032x}", oid, typestr, obj_decompressed_size, size_in_packfile, packfile_index, base_oid);
        } else {
            println!("{:032x} {}\t{} {} {}", oid, typestr, obj_decompressed_size, size_in_packfile, packfile_index);
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
