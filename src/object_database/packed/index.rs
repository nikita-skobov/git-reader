use std::{path::{Path, PathBuf}, io, fmt::Debug, mem::size_of, convert::TryInto, collections::{BTreeMap, HashMap}, fs::File};
use byteorder::{BigEndian, ByteOrder};
use crate::{ioerre, fs_helpers, object_id::{get_first_byte_of_oid, Oid, full_oid_to_u128_oid, full_slice_oid_to_u128_oid, full_oid_from_str, OidFull, hex_u128_to_str, PartialOid}, ioerr, object_database::{loose::UnparsedObject, PartialSearchResult}};
use memmap2::Mmap;
use super::{parse_pack_or_idx_id, PartiallyResolvedPackFile};
use super::{find_encoded_length, PackFileObjectType, apply_delta, open_pack_file};
use io::{Seek, Read, SeekFrom};

/// see: https://git-scm.com/docs/pack-format#_version_2_pack_idx_files_support_packs_larger_than_4_gib_and
pub const V2_IDX_SIGNATURE: [u8; 4] = [255, b't', b'O', b'c'];
pub const V2_IDX_SIGNATURE_LEN: usize = 4;
pub const V2_SKIP_VERSION_NUMBER_SIZE: usize = 4;
pub const V2_IDX_VERSION_NUMBER_BYTES: [u8; 4] = [0, 0, 0, 2];
pub const V2_IDX_VERSION_NUMBER: u32 = 2;
pub const FANOUT_LENGTH: usize = 256;
pub const FANOUT_ENTRY_SIZE: usize = 4;
pub const SHA1_SIZE: usize = 20;
pub const READ_INITIAL_BYTES: usize = V2_IDX_SIGNATURE_LEN + V2_SKIP_VERSION_NUMBER_SIZE + (FANOUT_ENTRY_SIZE * FANOUT_LENGTH);
/// according to docs, it looks like trailer is just 2 checksums?
pub const IDX_TRAILER_SIZE: usize = SHA1_SIZE * 2;
pub const MINIMAL_IDX_FILE_SIZE: usize = IDX_TRAILER_SIZE + FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
pub const V1_HEADER_SIZE: usize = FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
pub const V2_HEADER_SIZE: usize = FANOUT_ENTRY_SIZE * 2 + FANOUT_LENGTH * FANOUT_ENTRY_SIZE;
pub const N32_HIGH_BIT: u32 = 0b10000000_00000000_00000000_00000000;
pub const N32_SIZE: usize = size_of::<u32>();
pub const N64_SIZE: usize = size_of::<u64>();

#[derive(Debug)]
pub enum PartiallyResolvedPackAndIndex {
    /// pointer to index file
    Unresolved(PathBuf),

    /// The index file is resolved, an in memory,
    /// but the pack file may or may not be resolved yet.
    /// The IDXFile struct decides if/when to resolve
    /// the pack file it 'owns'
    IndexResolved(IDXFile),
}

pub struct IDXFile {
    pub fanout_table: [u32; 256],
    // this is the name of the index file.
    // we don't need this other than for debugging purposes..
    pub id: OidFull,
    pub version: IDXVersion,
    pub num_objects: u32,
    pub mmapped_file: Mmap,
    pub pack: PartiallyResolvedPackFile,
}

/// We implement debug manually because we
/// dont want the mmapped file to be debugged...
impl Debug for IDXFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IDXFile")
            .field("fanout_table", &self.fanout_table)
            .field("num_objects", &self.num_objects)
            .field("version", &self.version)
            .finish()
    }
}

impl IDXFile {
    pub fn load_pack(&mut self) -> io::Result<()> {
        let pack_path = match &self.pack {
            PartiallyResolvedPackFile::Unresolved(p) => p,
            // if already resolved, no need to do anything
            PartiallyResolvedPackFile::Resolved(_) => return Ok(()),
        };

        let pack = open_pack_file(pack_path, self.id)?;
        self.pack = PartiallyResolvedPackFile::Resolved(pack);
        Ok(())
    }

    /// return a map of indices in the packfile -> Oid at that index.
    /// Usually we are given an Oid and we want to find the index in the
    /// packfile where the data is of this object. But we can also get a map of the
    /// reverse. This is useful primarily for the verify-pack command
    /// where we read through the pack file, and then we want to know which oid
    /// is at the index we are reading.
    pub fn get_index_oid_map(&self) -> io::Result<BTreeMap<usize, Oid>> {
        let mut map = BTreeMap::new();

        let mut err_str = String::with_capacity(0);
        self.walk_all_oids(|oid| {
            // TODO: this is actually inefficient, because
            // if we are traversing the .idx file, we can already
            // find the offset relative to the position that we found this oid...
            // Im only doing this because of convenience...
            match self.get_packfile_index_of_oid(oid) {
                Ok(Some(i)) => {
                    map.insert(i, oid);
                }
                Ok(None) => {
                    err_str.push_str(&format!("\nFailed to find index of oid {}", hex_u128_to_str(oid)));
                }
                Err(e) => {
                    err_str.push_str(&format!("\nError while trying to find index of oid {}\n{}", hex_u128_to_str(oid), e));
                }
            }
            false
        });

        if !err_str.is_empty() {
            return ioerre!("{}", err_str);
        }
        Ok(map)
    }

    pub fn find_index_for(&self, oid: Oid) -> io::Result<Option<usize>> {
        let first_byte = get_first_byte_of_oid(oid) as usize;
        let mut start_search = if first_byte > 0 {
            self.fanout_table[first_byte - 1]
        } else {
            0
        } as usize;
        let mut end_search = self.fanout_table[first_byte] as usize;

        while start_search < end_search {
            let mid = (start_search + end_search) / 2;
            let mid_id = self.get_oid_at_index(mid)
                .ok_or_else(|| ioerr!("Invalid index for idx file: {}", mid))?;
            if oid < mid_id {
                end_search = mid;
            } else if oid > mid_id {
                start_search = mid + 1;
            } else {
                return Ok(Some(mid))
            }
        }

        Ok(None)
    }

    /// this function discards any errors and considers
    /// them to be false, ie: if theres an error we
    /// return false, because presumably if your oid required
    /// us to read some index out of bounds somehow, then
    /// that would mean this oid is not in this idx file.
    pub fn contains_oid(&self, oid: Oid) -> bool {
        match self.find_index_for(oid) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    pub fn get_all_oids(&self) -> Vec<Oid> {
        let mut oids = Vec::with_capacity(self.num_objects as usize);
        self.walk_all_oids(|oid| {
            oids.push(oid);
            // return false to indicate we dont want to
            // stop searching
            false
        });
        debug_assert!(
            oids.len() as u32 == self.num_objects,
            "Number of OIDs found is not what was expected"
        );
        oids
    }

    /// pass a callback that takes an oid that we found,
    /// and returns true if you want to stop searching.
    pub fn walk_all_oids(&self, cb: impl FnMut(Oid) -> bool) {
        let mut cb = cb;
        match self.version {
            // if we are a v2 idx file, then we just need to
            // go past the header/fanout table, and
            // then iterate 20 bytes at a time, each 20 bytes
            // is a full oid of an object that is in this idx file.
            IDXVersion::V2 => {
                let mut start_index = V2_HEADER_SIZE;
                for _ in 0..self.num_objects {
                    let desired_range = start_index..(start_index + SHA1_SIZE);
                    let full_sha = self.mmapped_file.get(desired_range);
                    let should_stop_iterating = match full_sha {
                        Some(sha_data) => {
                            // now create the oid from it:
                            let oid = full_slice_oid_to_u128_oid(sha_data);
                            let should_stop_iterating = cb(oid);
                            should_stop_iterating
                        }
                        None => {
                            // if we reached a point in the file
                            // where we cant get any more bytes,
                            // we can stop iterating here:
                            true
                        }
                    };
                    if should_stop_iterating { break; }
                    start_index += SHA1_SIZE;
                }
            }
            IDXVersion::V1 => {
                // theres 4 bytes of offset in each entry in v1 idx files.
                // so we skip the 4 bytes, get the next 20 bytes as the sha,
                // and then go ahead another 24 bytes to get the next one.
                let mut start_index = V1_HEADER_SIZE + FANOUT_ENTRY_SIZE;
                for _ in 0..self.num_objects {
                    let desired_range = start_index..(start_index + SHA1_SIZE);
                    let full_sha = self.mmapped_file.get(desired_range);
                    let should_stop_iterating = match full_sha {
                        Some(sha_data) => {
                            // now create the oid from it:
                            let oid = full_slice_oid_to_u128_oid(sha_data);
                            let should_stop_iterating = cb(oid);
                            should_stop_iterating
                        }
                        None => {
                            // if we reached a point in the file
                            // where we cant get any more bytes,
                            // we can stop iterating here:
                            true
                        }
                    };
                    if should_stop_iterating { break; }
                    start_index += SHA1_SIZE + FANOUT_ENTRY_SIZE;
                }
            }
        }
    }

    pub fn find_packfile_index_for(&self, index: usize) -> Option<u64> {
        match self.version {
            IDXVersion::V2 => {
                let start = self.offset_pack_offset_v2() + index * FANOUT_ENTRY_SIZE;
                let desired_range = start..(start + N32_SIZE);
                let desired_bytes = &self.mmapped_file.get(desired_range)?;
                self.pack_offset_from_offset_v2(desired_bytes, self.offset_pack_offset64_v2())
            }
            IDXVersion::V1 => {
                let start = V1_HEADER_SIZE + index * (FANOUT_ENTRY_SIZE + SHA1_SIZE);
                let desired_range = start..(start + FANOUT_ENTRY_SIZE);
                let desired_bytes = &self.mmapped_file.get(desired_range)?;
                Some(BigEndian::read_u32(desired_bytes) as u64)
            }
        }
    }

    pub fn get_oid_at_index(&self, index: usize) -> Option<Oid> {
        let start = match self.version {
            IDXVersion::V2 => V2_HEADER_SIZE + index * SHA1_SIZE,
            IDXVersion::V1 => V1_HEADER_SIZE + index * (FANOUT_ENTRY_SIZE + SHA1_SIZE) + FANOUT_ENTRY_SIZE,
        };
        let desired_range = start..start + SHA1_SIZE;
        let full_sha = self.mmapped_file.get(desired_range)?;
        Some(full_slice_oid_to_u128_oid(full_sha))
    }

    pub fn offset_crc32_v2(&self) -> usize {
        V2_HEADER_SIZE + self.num_objects as usize * SHA1_SIZE
    }

    pub fn offset_pack_offset_v2(&self) -> usize {
        self.offset_crc32_v2() + self.num_objects as usize * FANOUT_ENTRY_SIZE
    }

    pub fn offset_pack_offset64_v2(&self) -> usize {
        self.offset_pack_offset_v2() + self.num_objects as usize * N32_SIZE
    }

    pub fn pack_offset_from_offset_v2(
        &self,
        offset: &[u8],
        pack64_offset: usize
    ) -> Option<u64> {
        let ofs32 = BigEndian::read_u32(offset);
        let value = if (ofs32 & N32_HIGH_BIT) == N32_HIGH_BIT {
            let from = pack64_offset + (ofs32 ^ N32_HIGH_BIT) as usize * N64_SIZE;
            let desired_range = from..(from + N64_SIZE);
            let desired_bytes = self.mmapped_file.get(desired_range)?;
            BigEndian::read_u64(desired_bytes)
        } else {
            ofs32 as u64
        };
        Some(value)
    }

    pub fn try_find_match_from_partial(&self, partial_oid: PartialOid) -> PartialSearchResult {
        let first_byte = get_first_byte_of_oid(partial_oid.oid);

        let mut found_matches = vec![];
        self.walk_all_oids(|oid| {
            if partial_oid.matches(oid) {
                found_matches.push(oid);
            }

            // if the first byte of this oid is greater
            // than our first byte, then we can stop walking
            let first_byte_of_oid = get_first_byte_of_oid(oid);
            first_byte_of_oid > first_byte
        });

        match found_matches.len() {
            0 => PartialSearchResult::FoundNone,
            1 => PartialSearchResult::FoundMatch(found_matches[0]),
            _ => PartialSearchResult::FoundMultiple(found_matches),
        }
    }

    pub fn get_packfile_index_of_oid(&self, oid: Oid) -> io::Result<Option<usize>> {
        let idx_index = self.find_index_for(oid)?;
        let idx_index = match idx_index {
            Some(i) => i,
            None => return Ok(None),
        };
        // the idx_index is the index within the .idx file
        // where we will find the index of the packfile object:
        let pack_index = match self.find_packfile_index_for(idx_index) {
            Some(i) => i,
            None => return Ok(None),
        };
        let pack_index: usize = pack_index.try_into()
            .map_err(|_| ioerr!("Failed to convert a .idx index offset to a valid packfile index"))?;

        Ok(Some(pack_index))
    }

    /// this should only be called if you know
    /// this oid is in this packfile. call `contains_oid` first
    /// to know if its in here or not. Otherwise, this will
    /// return an error if the oid is not in here, which for some
    /// operations might not be an important error. ie: we don't
    /// differentiate between real errors like failing to read a file
    /// or getting an index out of bounds, vs an error of simply not
    /// finding this oid.
    /// ALSO it is an error to call this if the pack file has not been
    /// resolved yet.
    pub fn resolve_unparsed_object(&self, oid: Oid) -> io::Result<UnparsedObject> {
        let pack_index = self.get_packfile_index_of_oid(oid)?;
        let pack_index = pack_index.ok_or_else(|| ioerr!("Failed to find oid in {} this idx file", hex_u128_to_str(oid)))?;
        let pack = match &self.pack {
            PartiallyResolvedPackFile::Unresolved(p) => {
                return ioerre!("Pack file {:?} has not been resolved yet", p);
            }
            PartiallyResolvedPackFile::Resolved(p) => p,
        };
        let (
            obj_type,
            obj_size,
            obj_starts_at
        ) = pack.get_object_type_and_len_at_index(pack_index)?;
        let obj_size: usize = obj_size.try_into()
        .map_err(|_| ioerr!("Failed to convert an object size ({}) into a usize", obj_size))?;

        // the pack can resolve everything other than ref delta
        // objects. we have to first find that base object
        // and then pass it to the pack for it to resolve
        // the deltas.
        // for now, it is an error if this idx file does not
        // contain the base object, but in the future
        // there should be a way to locate a ref base object even
        // if its not in this idx file.
        if let PackFileObjectType::RefDelta(base_oid_full) = obj_type {
            // first we try to load the base object:
            let base_oid = full_oid_to_u128_oid(base_oid_full);
            let base_oid_str = hex_u128_to_str(base_oid);
            eprintln!("TRYING TO RESOLVE A BASE OBJ: {}", base_oid_str);
            let unparsed_object = self.resolve_unparsed_object(base_oid)?;
            let base_object_data = unparsed_object.payload;
            let base_object_type = unparsed_object.object_type;

            // next we load our data:
            let this_object_data = pack.get_decompressed_data_from_index(obj_size, obj_starts_at)?;
    
            // for our data, we need to extract the length, which
            // is again size encoded like the other cases:
            let (_base_size, num_read) = find_encoded_length(&this_object_data)
                .ok_or_else(|| ioerr!("Failed to find size of base object"))?;
            let this_object_data = &this_object_data[num_read..];
            let (our_size, num_read) = find_encoded_length(&this_object_data)
                .ok_or_else(|| ioerr!("Failed to find size of object"))?;
            let this_object_data = &this_object_data[num_read..];

            let data_out = apply_delta(&base_object_data, this_object_data, our_size)?;
            let unparsed_obj = UnparsedObject {
                object_type: base_object_type,
                payload: data_out
            };
            Ok(unparsed_obj)
        } else {
            pack.resolve_unparsed_object(obj_size, obj_starts_at, obj_type)
        }
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum IDXVersion {
    V1,
    V2,
}

/// inspired by both:
/// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L87
/// https://github.com/Byron/gitoxide/blob/157b6ff7b55ba2b7f8f90f66864212906426f8d7/git-pack/src/index/init.rs#L36
pub fn open_idx_file<P: AsRef<Path>>(
    path: P
) -> io::Result<IDXFile> {
    let idx_file_path = path.as_ref().to_path_buf();
    let pack_file_path = idx_file_path.with_extension("pack");
    if !pack_file_path.is_file() {
        return ioerre!("Failed to find corresponding pack file: {:?}", pack_file_path);
    }
    let mmapped = fs_helpers::get_mmapped_file(path)?;
    let idx_size = mmapped.len();
    if idx_size < MINIMAL_IDX_FILE_SIZE {
        return ioerre!("IDX file is too small to be a valid idx file");
    }
    let v2_idx_sig_len = V2_IDX_SIGNATURE.len();
    let version = if &mmapped[0..v2_idx_sig_len] == V2_IDX_SIGNATURE {
        // 4 byte version number... docs say it has to be == 2,
        // if we detected a V2 idx signature:
        let version_range = v2_idx_sig_len..(v2_idx_sig_len + V2_SKIP_VERSION_NUMBER_SIZE);
        let version_bytes = &mmapped[version_range];
        let version_number = BigEndian::read_u32(version_bytes);
        if version_number != V2_IDX_VERSION_NUMBER {
            return ioerre!("Invalid .idx version number. Expected version number of {}, found {}", V2_IDX_VERSION_NUMBER, version_number);
        }
        IDXVersion::V2
    } else {
        IDXVersion::V1
    };

    // now get the data, for v1 its the entirety
    // of the file, but for v2 its everything after the
    // first 8 bytes
    let data = match version {
        IDXVersion::V1 => &mmapped[..],
        IDXVersion::V2 => {
            let skip_index = v2_idx_sig_len + V2_SKIP_VERSION_NUMBER_SIZE;
            &mmapped[skip_index..]
        }
    };

    let mut fanout_table = [0; FANOUT_LENGTH];
    fill_fan(&mut fanout_table, data);
    let num_objects = fanout_table[FANOUT_LENGTH - 1];
    let idx_pack_id = parse_pack_or_idx_id(&idx_file_path)
        .ok_or_else(|| ioerr!("Failed to parse id from idx file: {:?}", idx_file_path))?;

    let idxfile = IDXFile {
        fanout_table,
        version,
        id: idx_pack_id,
        num_objects,
        mmapped_file: mmapped,
        pack: PartiallyResolvedPackFile::Unresolved(pack_file_path),
    };
    Ok(idxfile)
}

pub struct IDXFileLight {
    pub fanout_table: [u32; 256],
    // TODO: add this:
    // pub id: OidFull,
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
    let mmapped = fs_helpers::get_mmapped_file(path)?;
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

    let out = IDXFileLight {
        fanout_table,
        version,
        num_objects,
        file: mmapped,
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
