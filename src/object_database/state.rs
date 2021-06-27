
use flate2::Decompress;
use crate::{ioerr, object_id::{Oid, OidFull, oid_full_to_string_no_alloc, get_first_byte_of_oid, HEX_BYTES, hash_object_file_and_folder}, ioerre, fs_helpers};
use std::io;
use super::{main_sep_byte, MAX_PATH_TO_DB_LEN, packed::{open_idx_file_light, IDXFileLight, parse_pack_or_idx_id}, DoesMatch, FoundPackedLocation, FoundObjectLocation};

pub enum OwnedOrBorrowedMut<'a, T> {
    Owned(T),
    BorrowedMut(&'a mut T),
}

impl<'a, T> AsMut<T> for OwnedOrBorrowedMut<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        match self {
            OwnedOrBorrowedMut::Owned(ref mut c) => c,
            OwnedOrBorrowedMut::BorrowedMut(b) => b,
        }
    }
}

/// any object DB state should be able to:
/// - keep a mutable decompression object
///   to avoid re-allocating each time we want
///   to decompress an object
/// - have a way to get information from an IDX file.
///   at the bare minimum this would simply
///   open/read an IDX file, but a more advanced
///   state would be able to save this information.
pub trait State {
    type Idx: IDXState;

    fn get_decompressor(&mut self) -> &mut Decompress;
    fn get_idx_file(&mut self, id: OidFull) -> io::Result<OwnedOrBorrowedMut<Self::Idx>>;

    fn iter_loose_folder<F>(&mut self, folder_byte: u8, cb: &mut F) -> io::Result<()>
        where F: FnMut(Oid, &str, &str) -> bool;

    fn iter_known_packs<F>(&mut self, cb: &mut F) -> io::Result<()>
        where F: FnMut(&mut Self, OidFull) -> bool;

    /// this is necessary in order to prevent re-allocating pathbufs each time we
    /// wish to read a file. Instead, we can create a stack allocated array
    /// of bytes that contains the path to the object DB, and then
    /// convert that as a string. that is returned along with the count
    /// of bytes that is currently in this array.
    /// a simple implementation would be:
    /// ```
    /// let my_db_str = "../.git/objects/";
    /// let mut my_arr = [0; MAX_PATH_TO_DB_LEN];
    /// my_arr[0..my_db_str.len()].copy_from_slice(my_db_str.as_bytes())
    /// (my_db_str.len(), my_arr)
    /// ```
    fn get_path_to_db_as_bytes(&self) -> (usize, [u8; MAX_PATH_TO_DB_LEN]);

    /// helper function to get a stack allocated array of bytes
    /// that can be converted to a string.
    /// extend_by should be valid utf-8 slice.
    /// we extend our self.path_to_db_bytes by the extend by slice
    /// and return an array that can be turned into a stack
    /// allocated string, as well as the index that you should
    /// take the slice up to.
    #[inline(always)]
    fn get_static_path_str(&self, extend_by: &[u8]) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
        let (path_to_db_bytes_start, mut stack_arr) = self.get_path_to_db_as_bytes();
        let extend_num = extend_by.len();
        let take_slice_to = path_to_db_bytes_start + extend_num;
        stack_arr[path_to_db_bytes_start..take_slice_to].copy_from_slice(extend_by);
        (take_slice_to, stack_arr)
    }

    #[inline(always)]
    fn get_idx_file_str_array_from_hash(&self, hex_str: &[u8]) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
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
}

pub trait IDXState {
    fn find_oid_and_fanout_index(&mut self, oid: Oid) -> io::Result<usize>;
    fn find_packfile_index_from_fanout_index(&mut self, fanout_index: usize) -> Option<u64>;
    fn walk_all_oids_from<F>(&mut self, start_byte: Option<u8>, cb: F)
        where F: FnMut(Oid) -> bool;

    fn get_partial_matches_with_locations<F, P>(&mut self, start_byte: Option<u8>, partial: P, cb: &mut F)
        where F: FnMut(Oid, FoundObjectLocation) -> bool,
              P: DoesMatch;

    fn id(&self) -> OidFull;
}

impl IDXState for IDXFileLight {
    fn find_oid_and_fanout_index(&mut self, oid: Oid) -> io::Result<usize> {
        IDXFileLight::find_oid_and_fanout_index(self, oid)
    }

    fn find_packfile_index_from_fanout_index(&mut self, fanout_index: usize) -> Option<u64> {
        IDXFileLight::find_packfile_index_from_fanout_index(self, fanout_index)
    }

    fn id(&self) -> OidFull {
        self.id
    }

    fn walk_all_oids_from<F>(&mut self, start_byte: Option<u8>, cb: F)
        where F: FnMut(Oid) -> bool
    {
        IDXFileLight::walk_all_oids_from(self, start_byte, cb)
    }

    fn get_partial_matches_with_locations<F, P>(&mut self, start_byte: Option<u8>, partial: P, cb: &mut F)
        where F: FnMut(Oid, FoundObjectLocation) -> bool,
              P: DoesMatch
    {
        let partial_oid_first_byte = partial.get_first_byte();
        self.walk_all_oids_with_index_and_from(start_byte, |oid, oid_index| {
            let found_oid_first_byte = get_first_byte_of_oid(oid);
            if partial.matches(oid) {
                if let Some(i) = IDXFileLight::find_packfile_index_from_fanout_index(self, oid_index) {
                    let object_starts_at = i;
                    let location = FoundPackedLocation {
                        id: self.id,
                        object_starts_at,
                        oid_index,
                    };
                    let stop_searching = cb(oid, FoundObjectLocation::FoundPacked(location));
                    if stop_searching { return true; }
                }
                // TODO: what if its not found?
            }
            // if the oid first byte that we just found in the file
            // is greater than the first byte of our
            // partial oid, this means we can stop reading
            // because the .idx file is sorted by oid.
            found_oid_first_byte > partial_oid_first_byte
        });
    }
}

/// The minimum amount of state necessary to perform any object DB
/// operations. all it has is the path to where the object DB is,
/// and a decompressor that
pub struct MinState {
    pub path_to_db_bytes: [u8; MAX_PATH_TO_DB_LEN],
    pub path_to_db_bytes_start: usize,
    pub decompressor: Decompress,
}

impl MinState {
    pub fn new(path: &str) -> io::Result<MinState> {
        // hard to imagine a path would be longer than this right?...
        let p_len = path.len();
        // we probably wont extend the path_to_db by more than 60 chars ever...
        let max_extend_by = 60;
        if p_len >= MAX_PATH_TO_DB_LEN - max_extend_by {
            return ioerre!("Path '{}' is too long for us to represent it without allocations", path);
        }
        // we create a static array that contains the utf8 bytes
        // of the path string. We do this so that
        // we can create path strings of other files in the object DB
        // without allocating, ie: we can use this stack allocated
        // array to create strings like {path_to_db}/pack-whatever...
        let mut path_to_db_bytes = [0; MAX_PATH_TO_DB_LEN];
        path_to_db_bytes[0..p_len].copy_from_slice(path.as_bytes());
        path_to_db_bytes[p_len] = main_sep_byte();

        let out = MinState {
            path_to_db_bytes,
            path_to_db_bytes_start: p_len + 1,
            decompressor: Decompress::new(true),
        };
        Ok(out)
    }
}

impl State for MinState {
    type Idx = IDXFileLight;

    fn get_decompressor(&mut self) -> &mut Decompress {
        &mut self.decompressor
    }

    fn get_idx_file(&mut self, id: OidFull) -> io::Result<OwnedOrBorrowedMut<Self::Idx>> {
        // first form the "pack-{40hex}.idx" string array:
        let hex_str = oid_full_to_string_no_alloc(id);
        let (take_to, str_arr) = self.get_idx_file_str_array_from_hash(&hex_str);
        let idx_path = std::str::from_utf8(&str_arr[0..take_to])
            .map_err(|_| ioerr!("Failed to load idx file from id: {:32x?}", hex_str))?;
        let file = open_idx_file_light(idx_path)?;
        Ok(OwnedOrBorrowedMut::Owned(file))
    }

    fn get_path_to_db_as_bytes(&self) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
        (self.path_to_db_bytes_start, self.path_to_db_bytes)
    }

    fn iter_loose_folder<F>(&mut self, folder_byte: u8, cb: &mut F) -> io::Result<()>
        where F: FnMut(Oid, &str, &str) -> bool
    {
        let first_byte = folder_byte as usize;
        let hex_first_byte: [u8; 2] = HEX_BYTES[first_byte];
        let (take_index, big_str_array) = self.get_static_path_str(&hex_first_byte);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        
        // we know all of these HEX_BYTES are valid utf-8 sequences
        // so we can unwrap:
        let hex_str = std::str::from_utf8(&hex_first_byte).unwrap();
        let mut stop_searching = false;
        fs_helpers::search_folder_out(&search_path_str, |entry| {
            if stop_searching { return Ok(()); }
            let entryname = entry.file_name();
            let filename = match entryname.to_str() {
                Some(s) => s,
                None => return Ok(()),
            };
            let oid = match hash_object_file_and_folder(hex_str, &filename) {
                Ok(o) => o,
                Err(_) => { return Ok(()); }
            };
            stop_searching = cb(oid, search_path_str, filename);
            Ok(())
        })
    }

    fn iter_known_packs<F>(&mut self, cb: &mut F) -> io::Result<()>
        where F: FnMut(&mut Self, OidFull) -> bool
    {
        // first we load every .idx file we find in the database/packs
        // directory
        let packs_dir = b"pack";
        let (take_index, big_str_array) = self.get_static_path_str(packs_dir);
        let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        // println!("Searching {}", search_path_str);
        let mut stop_searching = false;
        fs_helpers::search_folder_out(&search_path_str, |entry| {
            if stop_searching { return Ok(()); }
            let filename = entry.file_name();
            let filename = match filename.to_str() {
                Some(s) => s,
                None => return Ok(()),
            };
            if ! filename.ends_with(".idx") {
                return Ok(());
            }
            let idx_id = match parse_pack_or_idx_id(filename) {
                Some(i) => i,
                None => return Ok(()),
            };
            stop_searching = cb(self, idx_id);
            Ok(())
        })
    }
}
