use std::{io, path::{Path, PathBuf}};
use crate::{fs_helpers, object_id::OidFull, ioerre, ioerr};
use byteorder::{ByteOrder, BigEndian};
use memmap2::Mmap;
use super::parse_pack_or_idx_id;


pub const PACK_SIGNATURE: &[u8; 4] = b"PACK";
pub const ACCEPTABLE_VERSION_NUMBERS: &[u32; 2] = &[2, 3];
/// 4 byte signature, 4 byte version, 4 byte number of objects,
pub const PACK_HEADER_SIZE: usize = 4 + 4 + 4;
/// 4 byte signature, 4 byte version, 4 byte number of objects, 4 bytes just for fun :)
pub const MINIMAL_PACK_FILE_SIZE: usize = PACK_HEADER_SIZE + 4;


pub enum PartiallyResolvedPackFile {
    Unresolved(PathBuf),
    Resolved(PackFile),
}

pub struct PackFile {
    // this is the name of the index (and also pack) file.
    // we don't need this other than for debugging purposes..
    pub id: OidFull,
    pub num_objects: u32,
    pub mmapped_file: Mmap,
}

/// Use this if you already read a .idx file and know the id.
/// otherwise if you don't know the ID yet, call
/// `open_pack_file_ex` and we will try to parse it for you.
pub fn open_pack_file<P: AsRef<Path>>(
    path: P,
    id: OidFull,
) -> io::Result<PackFile> {
    let mmapped = fs_helpers::get_mmapped_file(&path)?;
    let pack_size = mmapped.len();
    if pack_size < MINIMAL_PACK_FILE_SIZE {
        return ioerre!("Pack file {:?} is too small to be a valid pack file", path.as_ref());
    }
    let header = &mmapped[0..PACK_HEADER_SIZE];
    let signature = &header[0..4];
    if signature != PACK_SIGNATURE {
        return ioerre!("Pack file {:?} did not have valid signature of 'PACK'", path.as_ref());
    }
    let version_number = BigEndian::read_u32(&header[4..8]);
    if !ACCEPTABLE_VERSION_NUMBERS.contains(&version_number) {
        return ioerre!("Pack file {:?} version number '{}' is not valid", path.as_ref(), version_number);
    }

    let num_objects = BigEndian::read_u32(&header[8..12]);
    let packfile = PackFile {
        id,
        num_objects,
        mmapped_file: mmapped,
    };
    Ok(packfile)
}

/// Use this if you don't know the id of the pack file yet
/// and you wish to parse it from the filename.
/// Otherwise, use `open_pack_file` if you already
/// know the id of the .idx file.
pub fn open_pack_file_ex<P: AsRef<Path>>(
    path: P
) -> io::Result<PackFile> {
    let path = path.as_ref();
    let pack_id = parse_pack_or_idx_id(&path)
        .ok_or_else(|| ioerr!("Failed to parse id from pack file: {:?}", path))?;
    open_pack_file(path, pack_id)
}
