use std::{io, path::{Path, PathBuf}, convert::TryFrom};
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
/// the index of where the first object should be found at.
/// its just the pack header size because the header is not of variable length
pub const DATA_STARTS_AT: usize = PACK_HEADER_SIZE;

pub enum PartiallyResolvedPackFile {
    Unresolved(PathBuf),
    Resolved(PackFile),
}

#[derive(Debug)]
pub enum PackFileObjectType {
    Commit,
    Tree,
    Blob,
    Tag,
    OfsDelta,
    RefDelta,
}

impl TryFrom<u8> for PackFileObjectType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let t = match value {
            0 => return ioerre!("0 is an invalid type for a packfile object"),
            0b0_001_0000 => Self::Commit,
            0b0_010_0000 => Self::Tree,
            0b0_011_0000 => Self::Blob,
            0b0_100_0000 => Self::Tag,
            0b0_101_0000 => return ioerre!("5 is a reserved type, and therefore invalid"),
            0b0_110_0000 => Self::OfsDelta,
            0b0_111_0000 => Self::RefDelta,
            _ => return ioerre!("Invalid pack file object type"),
        };
        Ok(t)
    }
}

pub struct PackFile {
    // this is the name of the index (and also pack) file.
    // we don't need this other than for debugging purposes..
    pub id: OidFull,
    pub num_objects: u32,
    pub mmapped_file: Mmap,
}

impl PackFile {
    /// read the pack file starting at index, and try to parse
    /// the object type and length
    /// inspired by:
    /// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L299
    pub fn get_object_type_and_len_at_index(
        &self,
        index: usize
    ) -> io::Result<(PackFileObjectType, u128)> {
        // since the length of an object has a variable
        // length, we don't know how many bytes to read here.
        // However, we obviously need to represent the size of this
        // object as some unsigned integer, so the maximum size
        // we can represent it is with 128 bits.
        // the calculation for how many bits used to calculate the length
        // is as follows:
        // from the first byte we read 4 bits.
        // for every byte after that we read 7 bits.
        // therefore, if we read 18 bytes, then
        // 17 * 7 + 4 = 123, which implies that
        // the maximum number of bytes we should read
        // in order to fill a u128 is 18 bytes.
        // if we read 18 bytes and still fail to find
        // a byte whose MSB is 0, then something is seriously
        // wrong because there is no way any object
        // has a size larger than 2^123 bytes (astronomically high)...
        // we consider it an error if it takes us more than 18 bytes
        // to find the length of an object

        let try_read_size = 18;
        let try_read_range = index..(index + try_read_size);
        let try_parse_segment = self.mmapped_file.get(try_read_range)
            .ok_or_else(|| ioerr!("Failed to read packfile at index {}", index))?;
        // the first byte contains the type at the first
        // 4 bits, not including the MSB:
        let type_bits_mask = 0b0111_0000;
        // TODO: technically we are not checking
        // if the first byte has a 0 in its MSB, which
        // would indicate we should stop reading bytes...
        // is it possible an object's type and length
        // can be stored entirely in 1 byte?
        // ie: can an object have a length of 15 or less?
        let first_byte = try_parse_segment[0];
        let object_type_byte = first_byte & type_bits_mask;
        let object_type = PackFileObjectType::try_from(object_type_byte)?;

        // for the first byte, the length only exists
        // in the last 4 bits.
        let mut length: u128 = (first_byte & 0b0000_1111) as u128;
        // the initial shift is 4 bits because we have filled
        // the first 4 bits of the length variable,
        // so the next bits need to go to the left of these 4 bits.
        let mut shift = 4;
        let mut found_last_byte = false;
        // we just read the first byte above, so now
        // read every byte after it:
        for byte in &try_parse_segment[1..] {
            let byte = *byte;
            let mut should_break = false;
            if byte & 0b1000_0000 == 0 {
                // we reached a byte whose MSB is a 0,
                // therefore this is the last byte we should read
                should_break = true;
            }

            let least_7_bits = (byte & 0b0111_1111) as u128;
            // we shift it by the shift amount before
            // adding it to the length:
            length += least_7_bits << shift;
            // since now we are reading 7 bits at a time,
            // we shift the length by 7:
            shift += 7;

            if should_break {
                found_last_byte = true;
                break;
            }
        }

        if !found_last_byte {
            return ioerre!("Read {} bytes and failed to find a byte whose MSB is 0... Failed to parse object's variable length", try_read_size);
        }
        Ok((object_type, length))
    }
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
