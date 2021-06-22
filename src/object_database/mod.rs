use std::{path::{PathBuf, Path}, io, fs};
use crate::{ioerre, object_id::{oid_full_to_string, Oid, PartialOid, hex_u128_to_str, full_oid_to_u128_oid, get_first_byte_of_oid, HEX_BYTES, hash_object_file_and_folder, OidFull}, ioerr, fs_helpers};

pub mod loose;
use loose::*;

pub mod packed;
use packed::*;

/// A type alias for an ObjectDB that stores raw
/// data when resolved. ie: the data it stores
/// is the decompressed zlib stream that is in each
/// loose object file.
pub type UnparsedObjectDB = ObjectDB<PartiallyResolvedLooseObject>;

/// A type alias for an ObjectDB that stores actual parsed data
/// when resolved. Further refined by specifying a desired
/// type for T which determines how commits are parsed, ie: which
/// information is desired to be parsed from commits, and the rest is
/// not parsed. This type parameter does not apply to
/// trees, blobs, or tags because those do not
/// have alternate ways to parse them. trees need to be fully parsed
/// otherwise they are useless. Blobs and tags currently are not parsed
pub type ParsedObjectDB<T> = ObjectDB<PartiallyParsedLooseObject<T>>;

pub struct ObjectDB<T: Resolve> {
    pub loose: PartiallyResolvedLooseMap<T>,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    pub packs: Vec<PartiallyResolvedPackAndIndex>,
}

pub enum ReturnedObject<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<T: Resolve> ObjectDB<T> {
    /// path should be the absolute path to the objects folder
    /// ie: /.../.git/objects/
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<ObjectDB<T>> {
        let canon_path = path.as_ref().to_path_buf().canonicalize()?;
        let odb = ObjectDB {
            loose: PartiallyResolvedLooseMap::from_path(&canon_path)?,
            packs: get_vec_of_unresolved_packs(&canon_path)?,
        };
        Ok(odb)
    }

    /// get the object if we already resolved it,
    /// and if not, we will resolve it which is why we need to be
    /// mutable
    pub fn get_object_mut<'a>(&'a mut self, oid: Oid) -> io::Result<ReturnedObject<'a, T::Object>> {
        // first search if this oid is in the loose objects map
        let obj_in_loose = self.loose.get_object(oid)?;
        match obj_in_loose {
            Some(obj) => Ok(ReturnedObject::Borrowed(obj)),
            None => {
                return ioerre!("Oid: {} not found. TODO: need to implement searching through pack file", oid);
            }
        }
    }

    /// returns where an object is located.
    /// if an object is found in the loose db, the same oid is
    /// returned, otherwise, the oid of the packfile is returned
    /// if not found, returns None
    pub fn where_obj(&self, oid: Oid) -> Option<Oid> {
        if self.loose.contains_oid(oid) {
            return Some(oid);
        }

        for pack in self.packs.iter() {
            match pack {
                PartiallyResolvedPackAndIndex::IndexResolved(idx) => {
                    if idx.contains_oid(oid) {
                        let idx_oid = full_oid_to_u128_oid(idx.id);
                        return Some(idx_oid);
                    }
                }
                // cant check if it has an oid if its unresolved...
                PartiallyResolvedPackAndIndex::Unresolved(_) => {}
            }
        }

        None
    }

    /// get an object if it exists. We cannot resolve here
    /// because we are not mutable, so objects being not resolved
    /// is the same as them not existing... only use this
    /// if you resolved all objects ahead of time.
    pub fn get_object<'a>(&'a self, oid: Oid) -> io::Result<ReturnedObject<'a, T::Object>> {
        // first search if this oid is in the loose objects map
        let obj_in_loose = self.loose.get_object_existing(oid)?;
        if let Some(obj) = obj_in_loose {
            return Ok(ReturnedObject::Borrowed(obj));
        }

        // we failed to find it in the loose map, so now
        // we search through all of our index files to see if
        // it exists in one of them.
        // Note that this will fail to find it if we have not resolved
        // the index files beforehand.
        // If you want to optionally resolve the index files as we go, use
        // `get_object_mut` instead.
        let oid_str = hex_u128_to_str(oid);
        for idx_pack in self.packs.iter() {
            let idx = match idx_pack {
                PartiallyResolvedPackAndIndex::Unresolved(_) => {
                    // if its not resolved, skip this one
                    // because we cannot resolve it anyway
                    continue;
                }
                PartiallyResolvedPackAndIndex::IndexResolved(idx) => idx,
            };
            if !idx.contains_oid(oid) {
                continue;
            }

            // this idx contains our desired oid, so lets read it:
            let idx_str = oid_full_to_string(idx.id);
            let unparsed = idx.resolve_unparsed_object(oid)?;
            let obj = T::make_object_from_unparsed(unparsed)?;
            return Ok(ReturnedObject::Owned(obj));
        }

        return ioerre!("Oid: {} not found. TODO: need to implement searching through pack file", oid_str);
    }

    pub fn fully_resolve_all_packs(&mut self) -> io::Result<()> {
        fully_resolve_all_packs(&mut self.packs)
    }

    /// iterates the vec of partially resolved packs,
    /// and loads the .idx file/parses its header if it
    /// has not been resolved yet.
    pub fn resolve_all_index_files(&mut self) -> io::Result<()> {
        resolve_all_packs(&mut self.packs)
    }

    /// result a single .idx file at the given index in the packs vec.
    /// returns an error if either we failed to open/parse the .idx file
    /// or if the provided index is out of range
    pub fn resolve_index_file(&mut self, index: usize) -> io::Result<()> {
        let packs_len = self.packs.len();
        let pack = self.packs.get_mut(index)
            .ok_or_else(|| ioerr!("Packs vec has length of {}, but you provided an index of {}", packs_len, index))?;
        match pack {
            PartiallyResolvedPackAndIndex::Unresolved(path) => {
                let idx = open_idx_file(path)?;
                *pack = PartiallyResolvedPackAndIndex::IndexResolved(idx);
            }
            // it is already resolved, no need to do anything
            PartiallyResolvedPackAndIndex::IndexResolved(_) => {}
        }
        Ok(())
    }

    /// check through both the loose DB, and the packed DB
    /// to find all possible matches of a partial oid.
    /// NOTE that we cannot search through idx files that are unresolved...
    /// if you wish to search for ALL POSSIBLE matches, then you must
    /// resolve the idx files first.
    pub fn try_find_match_from_partial(&self, partial_oid: PartialOid) -> PartialSearchResult {
        let loose_result = self.loose.try_find_match_from_partial(partial_oid);

        let mut collect_results = loose_result;
        for pack in self.packs.iter() {
            match pack {
                PartiallyResolvedPackAndIndex::IndexResolved(idx) => {
                    let idx_result = idx.try_find_match_from_partial(partial_oid);
                    collect_results = collect_results.add(idx_result);
                }
                // cant check if it has an oid if its unresolved...
                PartiallyResolvedPackAndIndex::Unresolved(_) => {}
            }
        }
        collect_results
    }

    pub fn walk_all_oids<F>(&self, walk_cb: F)
        where F: FnMut(Oid)
    {
        let mut walk_cb = walk_cb;
        for key in self.loose.map.keys() {
            walk_cb(*key);
        }

        for pack in self.packs.iter() {
            match pack {
                PartiallyResolvedPackAndIndex::IndexResolved(idx) => {
                    idx.walk_all_oids(|oid| {
                        walk_cb(oid);
                        false
                    });
                }
                // cannot walk if its not resolved
                PartiallyResolvedPackAndIndex::Unresolved(_) => {}
            }
        }
    }
}

pub enum PartialSearchResult {
    FoundMatch(Oid),
    FoundMultiple(Vec<Oid>),
    FoundNone,
}

impl PartialSearchResult {
    pub fn add(self, other: PartialSearchResult) -> PartialSearchResult {
        let mut my_vec = match self {
            PartialSearchResult::FoundMatch(one) => vec![one],
            PartialSearchResult::FoundMultiple(v) => v,
            PartialSearchResult::FoundNone => vec![],
        };
        match other {
            PartialSearchResult::FoundMatch(one) => {
                my_vec.push(one);
            }
            PartialSearchResult::FoundMultiple(v) => {
                my_vec.extend(v);
            }
            PartialSearchResult::FoundNone => {}
        }

        match my_vec.len() {
            0 => PartialSearchResult::FoundNone,
            1 => PartialSearchResult::FoundMatch(my_vec[0]),
            _ => PartialSearchResult::FoundMultiple(my_vec),
        }
    }
}

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
pub fn main_sep_byte() -> u8 {
    match std::path::MAIN_SEPARATOR {
        // unix-like:
        '/' => 47,
        // otherwise windows:
        _ => 92,
    }
}

/// A fairly different interface than ObjectDB, the LightObjectDB
/// tries to minimize allocations at the cost of potentially
/// slightly slower performance. One key difference between the
/// light object DB is that it does not load all paths of loose objects,
/// instead, when you wish to load a loose object, it has to first
/// check if that loose object exists by making a file system call.
/// if done repeatedly, this would amount to significantly more calls
/// than using a regular ObjectDB, but a LightObjectDB is better
/// if you know you only need to lookup information once, as it requires
/// less allocations
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
    pub fn get_static_path_str(&self, extend_by: &[u8]) -> ([u8; MAX_PATH_TO_DB_LEN], usize) {
        let mut stack_arr = self.path_to_db_bytes;
        let extend_num = extend_by.len();
        let take_slice_to = self.path_to_db_bytes_start + extend_num;
        stack_arr[self.path_to_db_bytes_start..take_slice_to].copy_from_slice(extend_by);
        (stack_arr, take_slice_to)
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
        let packs_dir = b"pack";
        let (mut big_str_array, take_index) = self.get_static_path_str(packs_dir);
        big_str_array[take_index] = main_sep_byte();
        let file_name_len = idx_file_name.len();
        let new_size = take_index + 1 + file_name_len;
        let desired_range = (take_index + 1)..new_size;
        big_str_array[desired_range].copy_from_slice(idx_file_name.as_bytes());
        let search_path_str = std::str::from_utf8(&big_str_array[0..new_size])
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
