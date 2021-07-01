use std::{io, path::{Path, PathBuf}, convert::{TryInto, TryFrom}};
use crate::{fs_helpers, object_id::{oid_full_to_string, OidFull}, ioerre, ioerr, object_database::loose::{UnparsedObjectType, UnparsedObject, UNPARSED_PAYLOAD_STATIC_SIZE}};
use byteorder::{ByteOrder, BigEndian};
use memmap2::Mmap;
use super::{apply_delta, parse_pack_or_idx_id};
use flate2::{FlushDecompress, Decompress};
use tinyvec::{TinyVec, tiny_vec, ArrayVec};


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

/// Used to represent the type of a pack file object
/// without containing the necessary information within
/// the enum. Transform this into a PackFileObjectType
/// before returning.
#[derive(Debug)]
pub enum PackFileObjectTypeInner {
    Commit,
    Tree,
    Blob,
    Tag,
    OfsDelta,
    RefDelta,
}

#[derive(Debug)]
pub enum PackFileObjectType {
    Commit,
    Tree,
    Blob,
    Tag,
    /// the index of where the base object starts at
    OfsDelta(usize),
    /// the id of the object that should be used as the base
    RefDelta(OidFull),
}

impl PackFileObjectType {
    /// only used to convert between
    /// the simple types (Commit, Tree, Blob, and Tag)
    /// do not call this with OfsDelta, or RefDelta
    pub fn from_simple(simple: PackFileObjectTypeInner) -> Option<PackFileObjectType> {
        let out = match simple {
            PackFileObjectTypeInner::Commit => PackFileObjectType::Commit,
            PackFileObjectTypeInner::Tree => PackFileObjectType::Tree,
            PackFileObjectTypeInner::Blob => PackFileObjectType::Blob,
            PackFileObjectTypeInner::Tag => PackFileObjectType::Tag,
            PackFileObjectTypeInner::OfsDelta |
            PackFileObjectTypeInner::RefDelta => return None,
        };
        Some(out)
    }

    pub fn into_unparsed_type(&self) -> Option<UnparsedObjectType> {
        let out = match self {
            PackFileObjectType::Commit => UnparsedObjectType::Commit,
            PackFileObjectType::Tree => UnparsedObjectType::Tree,
            PackFileObjectType::Blob => UnparsedObjectType::Blob,
            PackFileObjectType::Tag => UnparsedObjectType::Tag,
            PackFileObjectType::OfsDelta(_) |
            PackFileObjectType::RefDelta(_) => return None,
        };
        Some(out)
    }
}

impl TryFrom<u8> for PackFileObjectTypeInner {
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
    /// the object type and length. returns
    /// (pack file object type, length of object, index where raw object starts)
    /// inspired by:
    /// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L299
    pub fn get_object_type_and_len_at_index(
        &self,
        index: usize
    ) -> io::Result<(PackFileObjectType, u128, usize)> {
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
        let first_byte = try_parse_segment[0];
        let msb_is_0 = first_byte & 0b1000_0000 == 0;
        let object_type_byte = first_byte & type_bits_mask;
        let object_type = PackFileObjectTypeInner::try_from(object_type_byte)?;

        // for the first byte, the length only exists
        // in the last 4 bits.
        let mut length: u128 = (first_byte & 0b0000_1111) as u128;
        // the initial shift is 4 bits because we have filled
        // the first 4 bits of the length variable,
        // so the next bits need to go to the left of these 4 bits.
        let mut shift = 4;
        let mut found_last_byte = false;
        let mut bytes_read = 1;
        // we just read the first byte above, so now
        // read every byte after it:
        if !msb_is_0 {
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
                bytes_read += 1;
    
                if should_break {
                    found_last_byte = true;
                    break;
                }
            }
        } else {
            // this condition means that we read 1 single byte
            // to find the variable length. WOW how efficient!
            // in 8 bits we found: the type of this object,
            // whether or not to continue reading the variable length,
            // and the length itself, which is stored in only 4 bits!
            found_last_byte = true;
        }

        if !found_last_byte {
            return ioerre!("Read {} bytes and failed to find a byte whose MSB is 0... Failed to parse object's variable length", try_read_size);
        }

        match object_type {
            PackFileObjectTypeInner::Commit |
            PackFileObjectTypeInner::Tree |
            PackFileObjectTypeInner::Blob |
            PackFileObjectTypeInner::Tag => {
                // we can unwrap because we know we are
                // passing a simple type:
                let out_obj = PackFileObjectType::from_simple(object_type)
                    .unwrap();
                return Ok((out_obj, length, index + bytes_read))
            }
            _ => {},
        }

        // now we perform further calculations if
        // its either an offset delta, or a ref delta.
        if let PackFileObjectTypeInner::OfsDelta = object_type {
            // I was a bit confused from the git documentation, but
            // after looking at:
            // https://github.com/Byron/gitoxide/blob/6200ed9ac5609c74de4254ab663c19cfe3591402/git-pack/src/data/entry/decode.rs#L99
            // I think the negative offset is calculated the same way we
            // calculated the above length, except this time
            // we dont have to skip 4 bits like we did above.
            // so its the same algorithm, just with 7 bits at a time.
            // I copied this function from Byron
            // because to be honest im not sure where the value += 1 comes from
            // but if it works, it works.
            let desired_range_start = index + bytes_read;
            let desired_range = desired_range_start..(desired_range_start + try_read_size);
            let negative_offset_data = self.mmapped_file.get(desired_range)
                .ok_or_else(|| ioerr!("Not enough bytes to read negative offset data from a delta offset object"))?;
            let (distance, more_bytes_read) = find_negative_offset(&negative_offset_data)
                .ok_or_else(|| ioerr!("Failed to parse negative offset data from a delta offset object"))?;
            if distance > index {
                return ioerre!("Detected a offset delta object has a negative offset of {} bytes, but that is farther than the beginning of the file", distance);
            }
            let base_obj_starts_at = index - distance;
            let obj_type = PackFileObjectType::OfsDelta(base_obj_starts_at);
            Ok((obj_type, length, desired_range_start + more_bytes_read))
        } else {
            // otherwise its a ref delta:
            // so here we know we just need to read the next 20
            // bytes which forms the sha hash:
            let mut id = OidFull::default();
            let full_sha_len = id.len();
            let start_reading_at = index + bytes_read;
            let sha_read_range = start_reading_at..(start_reading_at + full_sha_len);
            let sha_data = self.mmapped_file.get(sha_read_range)
                .ok_or_else(|| ioerr!("Detected a ref delta, but failed to read an additional 20 bytes for the SHA"))?;
            id.copy_from_slice(sha_data);
            let obj_type = PackFileObjectType::RefDelta(id);
            // the actual raw object resides immediately
            // after the 20 byte sha:
            Ok((obj_type, length, start_reading_at + full_sha_len))
        }
    }

    pub fn get_pack_size(&self) -> usize {
        self.mmapped_file.len()
    }

    /// return the decompressed data from an object at a given
    /// index. the `decompressed_size` should be the size of the output vec.
    /// Note: this ONLY decompressed data at an index and outputs
    /// a vec of desired decompressed size. It does not apply deltas.
    /// This is a convenience method to get out the raw data for each object, and
    /// then you can resolve deltas between the two as needed.
    pub fn get_decompressed_data_from_index(
        &self,
        decompressed_size: usize,
        starts_at: usize,
        decompressor: &mut Decompress,
    ) -> io::Result<TinyVec<[u8; UNPARSED_PAYLOAD_STATIC_SIZE]>> {
        // Is it guaranteed that compressed data is ALWAYS smaller
        // than the decompressed output? This is quite an assumption here...
        // to be safe, we will extend it by 128 bytes. But then
        // if we do that, we have to check if we reached the end of the file,
        // and reduce it if we've gone too far:
        let compressed_data_ends_at = starts_at + decompressed_size + 128;
        let compressed_data_ends_at = if compressed_data_ends_at > self.mmapped_file.len() {
            self.mmapped_file.len()
        } else {
            // we are good:
            compressed_data_ends_at
        };
        let compressed_data_range = starts_at..compressed_data_ends_at;
        let compressed_data = self.mmapped_file.get(compressed_data_range)
            .ok_or_else(|| ioerr!("Failed to read compressed data of pack file"))?;

        let mut out_vec = {
            let tinyout = if decompressed_size > UNPARSED_PAYLOAD_STATIC_SIZE {
                // too big to fit in array, allocate on heap:
                let v = vec![0; decompressed_size];
                TinyVec::Heap(v)
            } else {
                let a: [u8; UNPARSED_PAYLOAD_STATIC_SIZE] = [0; UNPARSED_PAYLOAD_STATIC_SIZE];
                TinyVec::Inline(ArrayVec::from_array_len(a, decompressed_size))
            };
            tinyout
        };
        // TODO: need to care about this decompressed state?
        // is it possible that we don't read into the entire
        // out vec in one go?
        decompressor.reset(true);
        let _decompressed_state = decompressor.decompress(
            compressed_data, &mut out_vec, FlushDecompress::None)?;
        let num_bytes_out = decompressor.total_out() as usize;
        if num_bytes_out != decompressed_size {
            return ioerre!("Failed to decompress {} bytes in one go. Only was able to decompress {} bytes. This is a bug on our end, please report this.", decompressed_size, num_bytes_out);
        }
        Ok(out_vec)
    }

    pub fn resolve_simple_object(
        &self,
        decompressor: &mut Decompress,
        decompressed_size: usize,
        starts_at: usize,
        unparsed_type: UnparsedObjectType,
    ) -> io::Result<UnparsedObject> {
        decompressor.reset(true);
        let data = self.get_decompressed_data_from_index(decompressed_size, starts_at, decompressor)?;
        let unparsed_obj = UnparsedObject {
            object_type: unparsed_type,
            payload: data,
        };
        Ok(unparsed_obj)
    }

    pub fn resolve_ofs_delta_object(
        &self,
        decompressor: &mut Decompress,
        decompressed_size: usize,
        starts_at: usize,
        base_starts_at: usize,
    ) -> io::Result<UnparsedObject> {
        let (
            next_obj_type,
            next_obj_size,
            next_obj_index
        ) = self.get_object_type_and_len_at_index(base_starts_at)?;
        let next_obj_size: usize = next_obj_size.try_into()
            .map_err(|_| ioerr!("Failed to convert {} into a usize. Either we failed at parsing this value, or your architecture does not support numbers this large", next_obj_size))?;
        decompressor.reset(true);
        let unparsed_object = self.resolve_unparsed_object(next_obj_size, next_obj_index, next_obj_type, decompressor)?;
        let this_object_data = self.get_decompressed_data_from_index(decompressed_size, starts_at, decompressor)?;
        let base_object_data = unparsed_object.payload;
        let base_object_type = unparsed_object.object_type;

        // for our data, we need to extract the length, which
        // is again size encoded like the other cases:
        let (_base_size, num_read) = find_encoded_length(&this_object_data)
            .ok_or_else(|| ioerr!("Failed to find size of base object"))?;
        let this_object_data = &this_object_data[num_read..];
        let (our_size, num_read) = find_encoded_length(&this_object_data)
            .ok_or_else(|| ioerr!("Failed to find size of object"))?;
        let this_object_data = &this_object_data[num_read..];

        // eprintln!("Going to look for delta data.");
        // eprintln!("Base object raw: {}", base_object_data.len());
        // eprintln!("Our delta data: {}", this_object_data.len());
        // eprintln!("We should be turned into a data of size: {}", our_size);
        let data_out = apply_delta(&base_object_data, this_object_data, our_size)?;
        let unparsed_obj_out = UnparsedObject {
            object_type: base_object_type,
            payload: data_out
        };
        Ok(unparsed_obj_out)
    }

    /// The continuation of `get_object_type_and_len_at_index`.
    /// Call this to fully resolve an object from a packfile using previously
    /// found information from the `get_object_type_and_len_at_index` call.
    /// This function will recursively resolve delta offsets (but not reference deltas!)
    /// and return an unparsed object that should be either a commit, tree, blob, or tag.
    pub fn resolve_unparsed_object(
        &self,
        decompressed_size: usize,
        starts_at: usize,
        object_type: PackFileObjectType,
        decompressor: &mut Decompress,
    ) -> io::Result<UnparsedObject> {
        match object_type {
            PackFileObjectType::Commit => {
                self.resolve_simple_object(
                    decompressor, decompressed_size, starts_at, UnparsedObjectType::Commit)
            }
            PackFileObjectType::Tree => {
                self.resolve_simple_object(
                    decompressor, decompressed_size, starts_at, UnparsedObjectType::Tree)
            }
            PackFileObjectType::Blob => {
                self.resolve_simple_object(
                    decompressor, decompressed_size, starts_at, UnparsedObjectType::Blob)
            }
            PackFileObjectType::Tag => {
                self.resolve_simple_object(
                    decompressor, decompressed_size, starts_at, UnparsedObjectType::Tag)
            }
            PackFileObjectType::OfsDelta(base_starts_at) => {
                self.resolve_ofs_delta_object(
                    decompressor, decompressed_size, starts_at, base_starts_at)
            }
            PackFileObjectType::RefDelta(id) => {
                let id_str = oid_full_to_string(id);
                return ioerre!("Not enough information to load base object of id {}. This base object needs to be resolved first by the .idx file before the pack file can parse it.", id_str);
            }
        }
    }
}

/// algorithm borrowed from:
/// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L220
/// Im not sure how/why this is different from
/// `find_negative_offset`. I thought they were supposed to do the
/// same thing but apparently not...
#[inline(always)]
pub fn find_encoded_length(d: &[u8]) -> Option<(usize, usize)> {
    let mut num_bytes_read = 1;
    let first_byte = d[0] as usize;
    let mut value = first_byte & 0x7f;
    if first_byte & 0b1000_0000 == 0 {
        return Some((value, num_bytes_read))
    }
    
    let mut shift = 0;
    let mut found_0_msb = false;
    for byte in &d[1..] {
        let byte = *byte;
        let mut should_break = false;
        if byte & 0b1000_0000 == 0 {
            should_break = true;
        }

        shift += 7;
        value |= ((byte & 0x7f) as usize) << shift;
        num_bytes_read += 1;

        if should_break {
            found_0_msb = true;
            break;
        }
    }

    if found_0_msb {
        Some((value, num_bytes_read))
    } else {
        None
    }
}

/// algorithm borrowed from:
/// https://github.com/Byron/gitoxide/blob/6200ed9ac5609c74de4254ab663c19cfe3591402/git-pack/src/data/entry/decode.rs#L99
/// Returns length, and number of bytes read
#[inline(always)]
pub fn find_negative_offset(d: &[u8]) -> Option<(usize, usize)> {
    let first_byte = d[0];
    let mut value = first_byte as usize & 0x7f;
    let mut num_bytes_read = 1;
    if first_byte & 0b1000_0000 == 0 {
        // we only needed 1 byte to calculate
        // the negative offset. pretty unlikely,
        // but we should check just in case:
        return Some((value, num_bytes_read))
    }

    // otherwise, read all remaining bytes
    // until we reach one that has a 0 as
    // the MSB:
    let mut found_0_msb = false;
    for byte in &d[1..] {
        let byte = *byte;
        let mut should_break = false;
        if byte & 0b1000_0000 == 0 {
            // this should be the last byte we read
            should_break = true;
        }
        value += 1;
        value = (value << 7) + (byte as usize & 0x7f);
        num_bytes_read += 1;
        if should_break {
            found_0_msb = true;
            break;
        }
    }

    if found_0_msb {
        Some((value, num_bytes_read))
    } else {
        None
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
