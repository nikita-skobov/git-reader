use std::{path::{PathBuf, Path}, io, convert::{TryInto, TryFrom}};
use crate::{ioerre, object_id::{Oid, PartialOid, full_oid_to_u128_oid, get_first_byte_of_oid, HEX_BYTES, hash_object_file_and_folder, OidFull, oid_full_to_string_no_alloc}, ioerr, fs_helpers};

pub mod loose;
use loose::*;

pub mod packed;
use packed::*;
use flate2::Decompress;
use state::{State, IDXState};

pub mod state;
pub mod light_state;


/// A trait used to see if 2 Oids match.
/// if both of the Oids are actually Oids then
/// its a simple equality check, but for PartialOid =?= Oid
/// check, then we need to check by shifting the Oid and then comparing.
pub trait DoesMatch: Copy {
    /// simple method to check if one Oid/PartialOid matches another.
    fn matches(&self, other: Oid) -> bool;
    /// Some operations require reading the first byte of an Oid.
    /// Regardless if this is an actual Oid, or a PartialOid, we should
    /// be able to get the first byte safely
    fn get_first_byte(&self) -> u8;
}

impl DoesMatch for Oid {
    #[inline(always)]
    fn matches(&self, other: Oid) -> bool {
        *self == other
    }
    #[inline(always)]
    fn get_first_byte(&self) -> u8 {
        get_first_byte_of_oid(*self)
    }
}

impl DoesMatch for PartialOid {
    #[inline(always)]
    fn matches(&self, other: Oid) -> bool {
        PartialOid::matches(self, other)
    }
    #[inline(always)]
    fn get_first_byte(&self) -> u8 {
        get_first_byte_of_oid(self.oid)
    }
}

pub const MAX_PATH_TO_DB_LEN: usize = 4096;

/// get the ascii value of the platform's main seperator.
/// / on Unix, \ on Windows
#[inline(always)]
pub fn main_sep_byte() -> u8 {
    match std::path::MAIN_SEPARATOR {
        // unix-like:
        '/' => 47,
        // otherwise windows:
        _ => 92,
    }
}

/// The LightObjectDB tries to minimize allocations at the cost of potentially
/// slightly slower performance.
/// if used repeatedly, this would amount to significantly more calls
/// to opening/closing files, but a LightObjectDB is better
/// if you know you only need to lookup information once, as it requires
/// less allocations.
/// It is also possible to use light object DB efficiently
/// by storing these files on your own, and using the appropriate
/// helper functions that take references to the idx/pack files
/// that you are holding on to.
pub struct LightObjectDB<'a> {
    /// Should be absolute path to /.../.git/objects/
    pub path_to_db: &'a str,
    pub path_to_db_bytes: [u8; MAX_PATH_TO_DB_LEN],
    pub path_to_db_bytes_start: usize,
}

/// a struct describing the information necessary
/// to read a packed object that was found in some index file.
#[derive(Debug, Copy, Clone)]
pub struct FoundPackedLocation {
    /// The full sha1 of the index file/pack file.
    /// ie: this is the Oid of "pack-{OidFull}.idx" or "pack-{OidFull}.pack"
    /// Note: this OidFull is the actual bytes of the sha1 hash. If you
    /// wish to read it as hex, you will need to convert it to a hex string.
    pub id: OidFull,
    /// The index within the packfile of where this object starts at.
    pub object_starts_at: u64,
    /// Which Nth index this oid is in the index file.
    /// eg: If we found the Oid as the 3rd Oid in the index file,
    /// this value is 3. This is useful if you wish to read
    /// the .idx file again, so you can jump right to this found oid.
    pub oid_index: usize,
}

/// An enum of where we could have possibly found an object.
#[derive(Debug, Clone)]
pub enum FoundObjectLocation {
    /// a simple path to where this loose object resides
    FoundLoose(PathBuf),
    /// a struct containing information necessary to read/locate
    /// this object in the pack file.
    FoundPacked(FoundPackedLocation),
}

impl<'a> LightObjectDB<'a> {
    pub fn new(p: &'a str) -> io::Result<LightObjectDB<'a>> {
        // hard to imagine a path would be longer than this right?...
        let p_len = p.len();
        // we probably wont extend the path_to_db by more than 60 chars ever...
        let max_extend_by = 60;
        if p_len >= MAX_PATH_TO_DB_LEN - max_extend_by {
            return ioerre!("Path '{}' is too long for us to represent it without allocations", p);
        }
        // we create a static array that contains the utf8 bytes
        // of the path string. We do this so that
        // we can create path strings of other files in the object DB
        // without allocating, ie: we can use this stack allocated
        // array to create strings like {path_to_db}/pack-whatever...
        let mut path_to_db_bytes = [0; MAX_PATH_TO_DB_LEN];
        path_to_db_bytes[0..p_len].copy_from_slice(p.as_bytes());
        path_to_db_bytes[p_len] = main_sep_byte();

        let out = LightObjectDB {
            path_to_db: p,
            path_to_db_bytes,
            path_to_db_bytes_start: p_len + 1,
        };
        Ok(out)
    }

    /// extend_by should be valid utf-8 slice.
    /// we extend our self.path_to_db_bytes by the extend by slice
    /// and return an array that can be turned into a stack
    /// allocated string, as well as the index that you should
    /// take the slice up to.
    #[inline(always)]
    pub fn get_static_path_str(&self, extend_by: &[u8]) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        let mut stack_arr = self.path_to_db_bytes;
        let extend_num = extend_by.len();
        let take_slice_to = self.path_to_db_bytes_start + extend_num;
        stack_arr[self.path_to_db_bytes_start..take_slice_to].copy_from_slice(extend_by);
        (stack_arr, take_slice_to)
    }

    #[inline(always)]
    pub fn get_pack_file_str_array_from_hash(&self, hex_str: &[u8]) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        // now we have our output str array:
        let mut out: [u8; 55] = [
            b'p', b'a', b'c', b'k', main_sep_byte(),
            b'p', b'a', b'c', b'k', b'-',
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            b'.', b'p', b'a', b'c', b'k'
        ];
        // and we copy our hex str to replace the 40 0s:
        out[10..50].copy_from_slice(&hex_str[0..40]);
        // now we have our filename, and pack/ part, we want
        // to append it to our base object db path:
        self.get_static_path_str(&out)
    }

    pub fn get_pack_file_str_array(&self, oidfull: OidFull) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        // first form the "pack-{40hex}.pack" string array:
        let hex_str = oid_full_to_string_no_alloc(oidfull);
        self.get_pack_file_str_array_from_hash(&hex_str)
    }

    pub fn get_idx_file_str_array(&self, oidfull: OidFull) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        // first form the "pack-{40hex}.idx" string array:
        let hex_str = oid_full_to_string_no_alloc(oidfull);
        self.get_idx_file_str_array_from_hash(&hex_str)
    }

    #[inline(always)]
    pub fn get_idx_file_str_array_from_hash(&self, hex_str: &[u8]) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        let mut out: [u8; 54] = [
            b'p', b'a', b'c', b'k', main_sep_byte(),
            b'p', b'a', b'c', b'k', b'-',
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            b'.', b'i', b'd', b'x'
        ];
        // and we copy our hex str to replace the 40 0s:
        out[10..50].copy_from_slice(&hex_str[0..40]);
        // now we have our filename, and pack/ part, we want
        // to append it to our base object db path:
        self.get_static_path_str(&out)
    }

    pub fn get_loose_object<F, P: AsRef<Path>>(
        &self,
        loose_obj_path: P,
    ) -> io::Result<F>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
    {
        // TODO: dont make this every time
        let mut decompressor = Decompress::new(true);
        let resolved_obj = read_raw_object(loose_obj_path, false, &mut decompressor)?;
        let transformed = F::try_from(resolved_obj)
            .map_err(|e| ioerr!("Failed to get loose object\n{}", e.to_string()))?;
        Ok(transformed)
    }

    /// This is a helper function to first:
    /// resolve an Oid given the idx file it should* be in,
    /// and once resolved, load it from the associated pack file.
    /// It is an error if the oid does not exist in this idx file.
    pub fn get_packed_object_from_oid<F, S>(
        &self,
        oid: Oid,
        pack_file: &PackFile,
        idx_id: OidFull,
        state: &mut S,
    ) -> io::Result<UnparsedObject>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
              S: State,
    {
        let mut idx_file = state.get_idx_file(idx_id)?;
        let idx_file = idx_file.as_mut();
        // this is the fanout index we use to find the
        // actual packfile index:
        let oid_index = idx_file.find_oid_and_fanout_index(oid)?;
        let pack_index = idx_file.find_packfile_index_from_fanout_index(oid_index)
            .ok_or_else(|| ioerr!("Found oid index, but failed to find packfile index offset for {:032x}", oid))?;
        let object_starts_at = pack_index;
        let location_info = FoundPackedLocation {
            id: idx_file.id(),
            object_starts_at,
            oid_index,
        };
        self.get_packed_object_packfile_loaded(&location_info, pack_file, state)
    }

    /// Like `get_packed_object` but takes a pack file that has
    /// already been loaded
    pub fn get_packed_object_packfile_loaded<F, S>(
        &self,
        packed_info: &FoundPackedLocation,
        pack: &PackFile,
        state: &mut S,
    ) -> io::Result<F>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
              S: State,
    {
        let obj_index: usize = packed_info.object_starts_at.try_into()
            .map_err(|_| ioerr!("Failed to convert u64 into usize in order to index the packfile. Your architecture might not allow {} to be represented as a usize.", packed_info.object_starts_at))?;
        let (
            obj_type, obj_size, obj_starts_at,
        ) = pack.get_object_type_and_len_at_index(obj_index)?;

        // obj size also needs to be converted to usize.
        let obj_size: usize = obj_size.try_into()
            .map_err(|_| ioerr!("Failed to convert u128 into usize in order to get object size. Your architecture might not allow {} to be represented as a usize.", obj_size))?;

        // if anything but Ref delta, we should be safe to just
        // call the pack and resolve it:
        let ref_id = match obj_type {
            PackFileObjectType::RefDelta(i) => i,
            _ => {
                let unparsed = pack.resolve_unparsed_object(obj_size, obj_starts_at, obj_type)?;
                let transformed = F::try_from(unparsed)
                    .map_err(|e| ioerr!("Failed to get packed object\n{}", e.to_string()))?;
                return Ok(transformed);
            }
        };

        // if its a ref delta, we need to get information
        // from the .idx file to get the index of
        // where its ref base object starts, and then try again.
        let base_oid = full_oid_to_u128_oid(ref_id);
        // we want the unparsed data, so we make sure
        // to specify that:
        let unparsed_object = self.get_packed_object_from_oid::<UnparsedObject, S>(
            base_oid, &pack, packed_info.id, state)?;
        // now that we have resolved the base object, we load our object:
        let base_object_data = unparsed_object.payload;
        let base_object_type = unparsed_object.object_type;

        // next we load our data:
        // TODO: dont make this every time:
        let mut decompressor = Decompress::new(true);
        let this_object_data = pack.get_decompressed_data_from_index(obj_size, obj_starts_at, &mut decompressor)?;

        // for our data, we need to extract the length:
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
        let transformed = F::try_from(unparsed_obj)
            .map_err(|e| ioerr!("Failed to get packed object\n{}", e.to_string()))?;
        Ok(transformed)
    }

    pub fn get_packed_object<F, S>(
        &self,
        packed_info: &FoundPackedLocation,
        state: &mut S,
    ) -> io::Result<F>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
              S: State,
    {
        // we need to first construct the path of this pack file:
        let (
            packfile_path_str_array, take_index
        ) = self.get_pack_file_str_array(packed_info.id);
        // make it into a string:
        let search_path_str = std::str::from_utf8(&packfile_path_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;

        // now read that file:
        let pack = open_pack_file(search_path_str, packed_info.id)?;
        self.get_packed_object_packfile_loaded(packed_info, &pack, state)
    }

    /// Get an object from its found location.
    /// This involves first parsing/extracting the raw
    /// data, and then transforming that data into your desired
    /// output generic F. If you just want the raw data, you can
    /// specify your generic as `UnparsedObject`, otherwise,
    /// you can specify one of the parsed objects that implements
    /// `UnparsedObject`
    pub fn get_object_from_location<F, S>(
        &self,
        location: FoundObjectLocation,
        state: &mut S,
    ) -> io::Result<F>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
              S: State,
    {
        match location {
            FoundObjectLocation::FoundLoose(path) => {
                self.get_loose_object(&path)
            }
            FoundObjectLocation::FoundPacked(info) => {
                self.get_packed_object(&info, state)
            }
        }
    }

    pub fn get_object_by_oid<F, S>(
        &self,
        oid: Oid,
        state: &mut S,
    ) -> io::Result<F>
        where F: TryFrom<UnparsedObject>,
              F::Error: ToString,
              S: State,
    {
        let (_, location) = self.find_first_matching_oid_with_location(oid)?;
        self.get_object_from_location(location, state)
    }

    pub fn find_matching_oids_loose<F>(
        &self,
        partial_oid: PartialOid,
        cb: &mut F,
    ) -> io::Result<()>
        where F: FnMut(Oid)
    {
        let first_byte = get_first_byte_of_oid(partial_oid.oid) as usize;
        let hex_first_byte: [u8; 2] = HEX_BYTES[first_byte];
        let (big_str_array, take_index) = self.get_static_path_str(&hex_first_byte);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        
        // we know all of these HEX_BYTES are valid utf-8 sequences
        // so we can unwrap:
        let hex_str = std::str::from_utf8(&hex_first_byte).unwrap();
        let _ = fs_helpers::search_folder(&search_path_str, |entry| -> Option<()> {
            let entryname = entry.file_name();
            let filename = match entryname.to_str() {
                Some(s) => s,
                None => return None,
            };
            let oid = match hash_object_file_and_folder(hex_str, &filename) {
                Ok(o) => o,
                Err(_) => { return None; }
            };
            if partial_oid.matches(oid) {
                cb(oid);
            }
            // TODO: otherwise if we failed to get str, should
            // we treat that as an error?
            None
        });
        Ok(())
    }

    /// like `find_matching_oids_loose` but in this callback,
    /// the full PathBuf to the matching oid object is also returned.
    /// The callback should return true if you want to stop searching
    pub fn find_matching_oids_loose_with_locations<F, M>(
        &self,
        partial_oid: M,
        cb: &mut F,
    ) -> io::Result<()>
        where F: FnMut(Oid, FoundObjectLocation) -> bool,
              M: DoesMatch
    {
        let first_byte = partial_oid.get_first_byte() as usize;
        let hex_first_byte: [u8; 2] = HEX_BYTES[first_byte];
        let (big_str_array, take_index) = self.get_static_path_str(&hex_first_byte);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        
        // we know all of these HEX_BYTES are valid utf-8 sequences
        // so we can unwrap:
        let hex_str = std::str::from_utf8(&hex_first_byte).unwrap();
        let mut stop_searching = false;
        let _ = fs_helpers::search_folder(&search_path_str, |entry| -> Option<()> {
            if stop_searching { return None; }
            let entryname = entry.file_name();
            let filename = match entryname.to_str() {
                Some(s) => s,
                None => return None,
            };
            let oid = match hash_object_file_and_folder(hex_str, &filename) {
                Ok(o) => o,
                Err(_) => { return None; }
            };
            if partial_oid.matches(oid) {
                // if we found a match, lets construct
                // a pathbuf from our current search folder,
                // and the filename of what we found:
                let mut full_pathbuf = PathBuf::from(search_path_str);
                full_pathbuf.push(filename);
                stop_searching = cb(oid, FoundObjectLocation::FoundLoose(full_pathbuf));
            }
            None
        });
        Ok(())
    }

    pub fn read_idx_file(
        &self,
        idx_file_name: &str,
    ) -> io::Result<IDXFileLight> {
        // our file name should be at least 45 chars long:
        // pack-{40hexchars}.idx
        // we want just the 40 hex chars:
        let idx_hex_str = idx_file_name.get(5..45)
            .ok_or_else(|| ioerr!("Failed to extract hex chars from idx file name: {}", idx_file_name))?;
        let (idx_str_array, take_to) = self.get_idx_file_str_array_from_hash(idx_hex_str.as_bytes());
        let search_path_str = std::str::from_utf8(&idx_str_array[0..take_to])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        // println!("reading idx file: {}", search_path_str);
        let idx_file = open_idx_file_light(search_path_str)?;
        Ok(idx_file)
    }

    pub fn find_matching_oids_packed<F>(
        &self,
        partial_oid: PartialOid,
        cb: &mut F,
    ) -> io::Result<()>
        where F: FnMut(Oid)
    {
        // first we load every .idx file we find in the database/packs
        // directory
        let partial_oid_first_byte = get_first_byte_of_oid(partial_oid.oid);
        let packs_dir = b"pack";
        let (big_str_array, take_index) = self.get_static_path_str(packs_dir);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        // println!("Searching {}", search_path_str);
        fs_helpers::search_folder(&search_path_str, |entry| -> Option<()> {
            let filename = entry.file_name();
            let filename = match filename.to_str() {
                Some(s) => s,
                None => return None,
            };
            if ! filename.ends_with(".idx") {
                return None;
            }
            let idx_file = match self.read_idx_file(filename) {
                Ok(f) => f,
                // TODO: should we stop all iteration
                // if a single idx file failed to read?
                // I think not? so here I just return None
                // and continue the iteration at the next idx filename
                Err(_) => { return None },
            };
            idx_file.walk_all_oids_from(Some(partial_oid_first_byte), |oid| {
                let found_oid_first_byte = get_first_byte_of_oid(oid);
                if partial_oid.matches(oid) {
                    cb(oid);
                }
                // if the oid first byte that we just found in the file
                // is greater than the first byte of our
                // partial oid, this means we can stop reading
                // because the .idx file is sorted by oid.
                found_oid_first_byte > partial_oid_first_byte
            });
            None
        })?;
        Ok(())
    }

    /// The callback should return true if you want to stop
    /// searching.
    pub fn find_matching_oids_packed_with_locations<F, M>(
        &self,
        partial_oid: M,
        cb: &mut F,
    ) -> io::Result<()>
        where F: FnMut(Oid, FoundObjectLocation) -> bool,
              M: DoesMatch
    {
        // first we load every .idx file we find in the database/packs
        // directory
        let partial_oid_first_byte = partial_oid.get_first_byte();
        let packs_dir = b"pack";
        let (big_str_array, take_index) = self.get_static_path_str(packs_dir);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        // println!("Searching {}", search_path_str);
        let mut last_error = Ok(());
        let mut stop_searching = false;
        fs_helpers::search_folder(&search_path_str, |entry| -> Option<()> {
            if stop_searching { return None; }
            let filename = entry.file_name();
            let filename = match filename.to_str() {
                Some(s) => s,
                None => return None,
            };
            if ! filename.ends_with(".idx") {
                return None;
            }
            let idx_file = match self.read_idx_file(filename) {
                Ok(f) => f,
                // TODO: should we stop all iteration
                // if a single idx file failed to read?
                // I think not? so here I just return None
                // and continue the iteration at the next idx filename
                Err(_) => { return None },
            };
            idx_file.walk_all_oids_with_index_and_from(Some(partial_oid_first_byte), |oid, oid_index| {
                let found_oid_first_byte = get_first_byte_of_oid(oid);
                if partial_oid.matches(oid) {
                    if let Some(i) = idx_file.find_packfile_index_from_fanout_index(oid_index) {
                        let object_starts_at = i;
                        let location = FoundPackedLocation {
                            id: idx_file.id,
                            object_starts_at,
                            oid_index,
                        };
                        stop_searching = cb(oid, FoundObjectLocation::FoundPacked(location));
                        if stop_searching { return true; }
                    } else {
                        last_error = ioerre!("Found an oid {:032x} but failed to find its packfile index", oid);
                    };
                }
                // if the oid first byte that we just found in the file
                // is greater than the first byte of our
                // partial oid, this means we can stop reading
                // because the .idx file is sorted by oid.
                found_oid_first_byte > partial_oid_first_byte
            });

            // we return None to the fs_helpers callback
            // so that it doesnt allocate any memory.
            None
        })?;
        Ok(())
    }

    pub fn find_matching_oids<F>(
        &self,
        partial_oid: PartialOid,
        cb: F,
    ) -> io::Result<()>
        where F: FnMut(Oid)
    {
        let mut cb = cb;
        self.find_matching_oids_loose(partial_oid, &mut cb)?;
        self.find_matching_oids_packed(partial_oid, &mut cb)?;

        Ok(())
    }

    pub fn find_matching_oids_with_locations<F, M>(
        &self,
        partial_oid: M,
        cb: F,
    ) -> io::Result<()>
        where F: FnMut(Oid, FoundObjectLocation),
              M: DoesMatch,
    {
        let mut cb = cb;
        let mut cb_wrapper = |oid, location| {
            cb(oid, location);
            false
        };
        self.find_matching_oids_loose_with_locations(partial_oid, &mut cb_wrapper)?;
        self.find_matching_oids_packed_with_locations(partial_oid, &mut cb_wrapper)?;
        Ok(())
    }

    pub fn find_first_matching_oid_with_location<M>(
        &self,
        partial_oid: M,
    ) -> io::Result<(Oid, FoundObjectLocation)>
        where M: DoesMatch
    {
        let mut found: Option<(Oid, FoundObjectLocation)> = None;
        let mut cb_wrapper = |oid, location| {
            found = Some((oid, location));
            true
        };
        self.find_matching_oids_loose_with_locations(partial_oid, &mut cb_wrapper)?;
        if let Some(f) = found {
            return Ok(f);
        }
        let mut found: Option<(Oid, FoundObjectLocation)> = None;
        let mut cb_wrapper = |oid, location| {
            found = Some((oid, location));
            true
        };
        self.find_matching_oids_packed_with_locations(partial_oid, &mut cb_wrapper)?;
        match found {
            Some(f) => Ok(f),
            None => {
                // TODO: should add debug requirement for M so we can print which
                // one we failed to find...
                return ioerre!("Failed to find a matching oid/location");
            }
        }
    }
}
