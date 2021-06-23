use std::{path::Path, io, fmt::Debug, mem::size_of};
use byteorder::{BigEndian, ByteOrder};
use crate::{ioerre, fs_helpers, object_id::{get_first_byte_of_oid, Oid, full_slice_oid_to_u128_oid, OidFull}, ioerr};
use memmap2::Mmap;
use super::parse_pack_or_idx_id;

/// see: https://git-scm.com/docs/pack-format#_version_2_pack_idx_files_support_packs_larger_than_4_gib_and
const V2_IDX_SIGNATURE: [u8; 4] = [255, b't', b'O', b'c'];
const V2_IDX_SIGNATURE_LEN: usize = 4;
const V2_SKIP_VERSION_NUMBER_SIZE: usize = 4;
const V2_IDX_VERSION_NUMBER: u32 = 2;
const FANOUT_LENGTH: usize = 256;
const FANOUT_ENTRY_SIZE: usize = 4;
const SHA1_SIZE: usize = 20;
const READ_INITIAL_BYTES: usize = V2_IDX_SIGNATURE_LEN + V2_SKIP_VERSION_NUMBER_SIZE + (FANOUT_ENTRY_SIZE * FANOUT_LENGTH);
/// according to docs, it looks like trailer is just 2 checksums?
const IDX_TRAILER_SIZE: usize = SHA1_SIZE * 2;
const MINIMAL_IDX_FILE_SIZE: usize = IDX_TRAILER_SIZE + FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
const V1_HEADER_SIZE: usize = FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
const V2_HEADER_SIZE: usize = FANOUT_ENTRY_SIZE * 2 + FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
const N64_SIZE: usize = size_of::<u64>();

#[derive(Debug, PartialOrd, PartialEq)]
pub enum IDXVersion {
    V1,
    V2,
}

pub struct IDXFileLight {
    pub fanout_table: [u32; 256],
    pub id: OidFull,
    pub version: IDXVersion,
    pub num_objects: usize,
    pub file: Mmap,
}

impl IDXFileLight {
    /// For V2 idx files, the oid starts at every index, and has an offset of 20 bytes.
    /// ie: there is no padding or anything. you just read 20 bytes at a time, and each
    /// 20 bytes is an Oid. the V2 idx file looks like:
    /// [4 byte magic number]
    /// [4 byte version number]
    /// [256 entries * 4 bytes each] // fanout table
    /// [oid_00] // 20 bytes
    /// [oid_01] // 20 bytes
    /// ... // until the last oid, which
    /// we know how many oids there are from the last value
    /// of the fanout table. ie: `num_oids = fanout_table.last();`
    /// The purpose of this function is to take an index of an oid you want,
    /// ie: I want the 3rd Oid in this file, and return the actual index
    /// where that Oid starts at. Which for v2 idx files, its just
    /// V2_HEADER_SIZE + (fanout_index * 20_bytes_is_size_of_each_oid)
    #[inline(always)]
    pub fn get_oid_starting_index_from_fanout_index_v2(&self, fanout_index: usize) -> usize {
        V2_HEADER_SIZE + (fanout_index * SHA1_SIZE)
    }

    /// Similarly to the `get_oid_starting_index_from_fanout_index_v2` function,
    /// this accomplishes the same goal (find the starting index of an oid, given
    /// the N-index of which Oid you are looking for, ie: I want the 3rd Oid...).
    /// However, unlike the v2 function, the calculation here is different because
    /// V1 idx files store this information differently. A V1 idx file looks like:
    /// [256 entries * 4 bytes each] // fanout table
    /// [4 byte offset of where this oid exists in the packfile][oid_00] // 4 + 20 bytes
    /// [4 byte offset of where this oid exists in the packfile][oid_01] // 4 + 20 bytes
    /// ... // until the last oid...
    /// so the calculation here is:
    /// V1_header_size + 4 + (fanout_index * 24)
    #[inline(always)]
    pub fn get_oid_starting_index_from_fanout_index_v1(&self, fanout_index: usize) -> usize {
        V1_HEADER_SIZE + FANOUT_ENTRY_SIZE + (fanout_index * (FANOUT_ENTRY_SIZE + SHA1_SIZE))
    }

    pub fn get_oid_starting_index_from_fanout_index(&self, fanout_index: usize) -> usize {
        match self.version {
            IDXVersion::V1 => {
                self.get_oid_starting_index_from_fanout_index_v1(fanout_index)
            }
            IDXVersion::V2 => {
                self.get_oid_starting_index_from_fanout_index_v2(fanout_index)
            }
        }
    }

    /// given a fanout_index, (ie: I want the 3rd Oid => fanout_index = 3),
    /// find the offset of where that object begins in the associated packfile.
    /// for V1 .idx files, this is simply the 4 bytes that come directly before
    /// the Oid. eg: if you want the 3rd Oid, you navigate to the 3rd entry, and
    /// the entry looks like:
    /// [4 byte packfile offset][oid_03] // 4 + 20 bytes.
    /// so here we want to read these first 4 bytes in network order.
    #[inline(always)]
    pub fn find_packfile_index_from_fanout_index_v1(&self, fanout_index: usize) -> Option<u64> {
        let oid_start = self.get_oid_starting_index_from_fanout_index_v1(fanout_index);
        // we subtract 4 because we dont want the oid index, but the 4 bytes before the oid:
        let offset_start = oid_start - FANOUT_ENTRY_SIZE;
        let desired_range = offset_start..oid_start;
        let desired_bytes = &self.file.get(desired_range)?;
        Some(BigEndian::read_u32(desired_bytes) as u64)
    }

    /// given a fanout_index, (ie: I want the 4th Oid => fanout_index = 4),
    /// find the offset of where that object begins in the associated pakcfile.
    /// for V2 .idx files, this involves reading from the end section of the .idx file
    /// where theres a table of 4 byte values, and each of these values is actually 31 bits.
    /// if the MSB is set, then that indicates we should actually read from the next table
    /// which has 8 byte offsets.
    #[inline(always)]
    pub fn find_packfile_index_from_fanout_index_v2(&self, fanout_index: usize) -> Option<u64> {
        let oid_table_starts_at = V2_HEADER_SIZE;
        let crc_table_starts_at = oid_table_starts_at + (self.num_objects * SHA1_SIZE);
        let four_byte_offset_table_starts_at = crc_table_starts_at + (self.num_objects * FANOUT_ENTRY_SIZE);

        let this_entry_starts = four_byte_offset_table_starts_at + (fanout_index * FANOUT_ENTRY_SIZE);
        let desired_range = this_entry_starts..(this_entry_starts + FANOUT_ENTRY_SIZE);
        let desired_bytes = self.file.get(desired_range)?;
        let four_byte_offset = BigEndian::read_u32(&desired_bytes);
        // if the MSB is not set, then we are done. the value we
        // read is the offset in the packfile
        if four_byte_offset & 0b10000000_00000000_00000000_00000000 == 0 {
            return Some(four_byte_offset as u64);
        }

        // otherwise, the MSB is set, so now we treat this four_byte_offset
        // as actually the index of the 8 byte table:
        // first we need to remove that MSB:
        let eight_byte_table_index = four_byte_offset ^ 0b10000000_00000000_00000000_00000000;
        let eight_byte_table_starts_at = four_byte_offset_table_starts_at + (self.num_objects * FANOUT_ENTRY_SIZE);
        let this_entry_starts = eight_byte_table_starts_at + (eight_byte_table_index as usize) * N64_SIZE;
        let desired_range = this_entry_starts..(this_entry_starts + N64_SIZE);
        let desired_bytes = self.file.get(desired_range)?;
        Some(BigEndian::read_u64(desired_bytes))
    }

    pub fn find_packfile_index_from_fanout_index(&self, fanout_index: usize) -> Option<u64> {
        match self.version {
            IDXVersion::V1 => {
                self.find_packfile_index_from_fanout_index_v1(fanout_index)
            }
            IDXVersion::V2 => {
                self.find_packfile_index_from_fanout_index_v2(fanout_index)
            }
        }
    }

    /// gets the CRC32 value from the CRC32 table of the corresponding fanout entry.
    /// NOTE: the CRC32 table only exists in V2 format. SO this unchecked
    /// function does not check if we are in V2, and thus you can be getting bogus
    /// CRC32 values... If you want to use a function that will check
    /// for V2, then use: `get_crc32_from_fanout_index`
    pub fn get_crc32_from_fanout_index_unchecked(&self, fanout_index: usize) -> Option<u32> {
        let oid_table_starts_at = V2_HEADER_SIZE;
        let crc_table_starts_at = oid_table_starts_at + (self.num_objects * SHA1_SIZE);
        let this_entry_starts = crc_table_starts_at + (fanout_index * FANOUT_ENTRY_SIZE);
        let desired_range = this_entry_starts..(this_entry_starts + FANOUT_ENTRY_SIZE);
        let desired_bytes = self.file.get(desired_range)?;
        let crc_value = BigEndian::read_u32(&desired_bytes);
        Some(crc_value)
    }

    /// Returns None if not on V2 idx, otherwise
    /// calls `get_crc32_from_fanout_index_unchecked`
    pub fn get_crc32_from_fanout_index(&self, fanout_index: usize) -> Option<u32> {
        if let IDXVersion::V2 = self.version {
            self.get_crc32_from_fanout_index_unchecked(fanout_index)
        } else {
            None
        }
    }

    /// Like `walk_all_oids_from`, but also passes
    /// the current fanout index of this oid. This fanout index
    /// can be passed to find_packfile_index_from_fanout_index() in order
    /// to find the packfile index where this object resides.
    pub fn walk_all_oids_with_index_and_from(
        &self,
        start_byte: Option<u8>,
        cb: impl FnMut(Oid, usize) -> bool
    ) {
        let mut cb = cb;
        let start_fanout_index = match start_byte {
            Some(first_byte) => {
                let first_byte = first_byte as usize;
                if first_byte > 0 {
                    self.fanout_table[first_byte - 1]
                } else {
                    0
                }    
            }
            None => 0,
        };

        let start_fanout_index = start_fanout_index as usize;
        // now we know which Nth oid we want, so now find the index of this oid,
        // as well as establish how many bytes we need to skip each time we advance to
        // the (N + 1)th oid.
        let (mut start_index, seek_up) = match self.version {
            IDXVersion::V1 => {
                let start_i = self.get_oid_starting_index_from_fanout_index_v1(start_fanout_index);
                // v1 requires a seekup of 4 bytes + 20 bytes for the oid
                (start_i, FANOUT_ENTRY_SIZE + SHA1_SIZE)
            }
            IDXVersion::V2 => {
                let start_i = self.get_oid_starting_index_from_fanout_index_v2(start_fanout_index);
                // v2 doesnt require up other than the oid size. if we read 20 bytes
                // the next oid is at the next 20 bytes immediately after
                (start_i, 0 + SHA1_SIZE)
            }
        };

        let mut current_fanout_index = start_fanout_index;
        for _ in 0..self.num_objects {
            // we always read SHA1_SIZE:
            let sha_bytes = &self.file[start_index..(start_index + SHA1_SIZE)];
            let oid = full_slice_oid_to_u128_oid(&sha_bytes);
            let should_stop_iterating = cb(oid, current_fanout_index);
            if should_stop_iterating { break; }

            // if V1, we have to seek ahead 4 bytes to skip
            // the offsets, but in V2, we just read the next SHA
            start_index += seek_up;
            current_fanout_index += 1;
        }
    }

    /// Returns Ok(usize) if the Oid exists,
    /// and if we were able to find its fanout index, ie (this is
    /// the nth oid...).
    pub fn find_oid_and_fanout_index(
        &self,
        oid: Oid
    ) -> io::Result<usize> {
        let mut found = None;
        let first_byte = get_first_byte_of_oid(oid);
        self.walk_all_oids_with_index_and_from(Some(first_byte), |found_oid, fanout_index| {
            if found_oid == oid {
                found = Some(fanout_index);
                // indicate we want to stop iterating
                return true;
            }
            false
        });
        match found {
            Some(i) => Ok(i),
            None => {
                return ioerre!("Failed to find index of oid {:032x}", oid);
            }
        }
    }

    /// pass a callback that takes an oid that we found,
    /// and returns true if you want to stop searching.
    /// if start_byte is some byte, we look for it in the fanout table
    /// and start our search there. Otherwise, if start_byte is None,
    /// we traverse all oids. This function can be used for both collecting
    /// all oids, or efficiently searching for a specific one.
    pub fn walk_all_oids_from(
        &self,
        start_byte: Option<u8>,
        cb: impl FnMut(Oid) -> bool
    ) {
        let mut cb = cb;
        self.walk_all_oids_with_index_and_from(start_byte, |oid, _| {
            cb(oid)
        })
    }
}

pub fn open_idx_file_light<P: AsRef<Path>>(
    path: P
) -> io::Result<IDXFileLight> {
    // let mut fhandle = fs_helpers::get_readonly_handle(&path)?;
    let mmapped = fs_helpers::get_mmapped_file(&path)?;
    let file_size = mmapped.len() as usize;
    if file_size < MINIMAL_IDX_FILE_SIZE {
        return ioerre!("IDX file is too small to be a valid idx file");
    }

    // read enough bytes to check for v2 and the fanout table.
    let read_bytes = &mmapped[0..READ_INITIAL_BYTES];
    let (version, num_objects, fanout_table) = if read_bytes[0..V2_IDX_SIGNATURE_LEN] == V2_IDX_SIGNATURE {
        // 4 byte version number... docs say it has to be == 2,
        // if we detected a V2 idx signature:
        let version_bytes = &read_bytes[V2_IDX_SIGNATURE_LEN..(V2_IDX_SIGNATURE_LEN + V2_SKIP_VERSION_NUMBER_SIZE)];
        let version_number = BigEndian::read_u32(&version_bytes);
        if version_number != V2_IDX_VERSION_NUMBER {
            return ioerre!("Invalid .idx version number. Expected version number of {}, found {}", V2_IDX_VERSION_NUMBER, version_number);
        }
        let fanout_starts = V2_IDX_SIGNATURE_LEN + V2_SKIP_VERSION_NUMBER_SIZE;
        let fanout_len = FANOUT_ENTRY_SIZE * FANOUT_LENGTH;
        let fanout_range = fanout_starts..(fanout_starts + fanout_len);
        let mut fanout_table = [0; FANOUT_LENGTH];
        fill_fan(&mut fanout_table, &read_bytes[fanout_range]);
        let num_objects = fanout_table[FANOUT_LENGTH - 1] as usize;
        (IDXVersion::V2, num_objects, fanout_table)
    } else {
        let mut fanout_table = [0; FANOUT_LENGTH];
        fill_fan(&mut fanout_table, &read_bytes);
        let num_objects = fanout_table[FANOUT_LENGTH - 1] as usize;
        (IDXVersion::V1, num_objects, fanout_table)
    };

    let idx_id = parse_pack_or_idx_id(path)
        .ok_or_else(|| ioerr!("Failed to parse idx idx"))?;

    let out = IDXFileLight {
        fanout_table,
        version,
        num_objects,
        file: mmapped,
        id: idx_id,
    };
    Ok(out)
}


/// taken from:
/// https://github.com/Byron/gitoxide/blob/157b6ff7b55ba2b7f8f90f66864212906426f8d7/git-pack/src/index/init.rs#L84
/// and slightly modified: we take a pre-instantiated fan, and fill it in.
/// Also, I cant figure out why we care about returning bytes read?
fn fill_fan(fan: &mut [u32; FANOUT_LENGTH], d: &[u8]) -> usize {
    for (c, f) in d.chunks(FANOUT_ENTRY_SIZE).zip(fan.iter_mut()) {
        *f = BigEndian::read_u32(c);
    }
    FANOUT_LENGTH * FANOUT_ENTRY_SIZE
}
