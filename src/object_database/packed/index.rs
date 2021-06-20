use std::{path::{Path, PathBuf}, io, fmt::Debug, mem::size_of, convert::TryInto};
use byteorder::{BigEndian, ByteOrder};
use crate::{ioerre, fs_helpers, object_id::{get_first_byte_of_oid, Oid, full_oid_to_u128_oid, full_slice_oid_to_u128_oid, full_oid_from_str, OidFull, hex_u128_to_str, PartialOid}, ioerr, object_database::{loose::UnparsedObject, PartialSearchResult}};
use memmap2::Mmap;
use super::{parse_pack_or_idx_id, PartiallyResolvedPackFile};
use super::{find_encoded_length, PackFileObjectType, apply_delta};

/// see: https://git-scm.com/docs/pack-format#_version_2_pack_idx_files_support_packs_larger_than_4_gib_and
pub const V2_IDX_SIGNATURE: [u8; 4] = [255, b't', b'O', b'c'];
pub const V2_SKIP_VERSION_NUMBER_SIZE: usize = 4;
pub const V2_IDX_VERSION_NUMBER_BYTES: [u8; 4] = [0, 0, 0, 2];
pub const V2_IDX_VERSION_NUMBER: u32 = 2;
pub const FANOUT_LENGTH: usize = 256;
pub const FANOUT_ENTRY_SIZE: usize = 4;
pub const SHA1_SIZE: usize = 20;
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

#[derive(Debug)]
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
