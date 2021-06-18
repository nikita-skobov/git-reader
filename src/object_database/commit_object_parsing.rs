use crate::{ioerre, object_id::{Oid}, ioerr};
use std::io;

pub enum ParseState {
    Tree,
    Parents,
    Author,
    Committer,
    Message,
}

// TODO: use a trait
// where each of the commit objects implements parse...
// pub trait ParseCommit {
//     fn parse_inner<T: ParseCommit>(raw: &[u8], current_index: &mut usize) -> io::Result<T>;

//     fn parse<T: ParseCommit>(raw: &[u8]) -> io::Result<T> {
//         let mut index = 0;
//         T::parse_inner(raw, &mut index)
//     }
// }

/// The reason we use `parent_one`, `parent_two`
/// and then have a seperate `extra_parents` vec
/// is to minimize heap allocations. Most commits will have
/// one or two parents, and then an empty `extra_parents` vec
/// will not need heap allocation, which should make
/// us a lot faster at the cost of about 30% more memory usage.
/// TODO: what other filtered versions do we want?
/// TODO: implement parsing
pub struct CommitFull {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub author: String,
    pub committer: String,
    pub message: String,
}

/// TODO: implement parsing
pub struct CommitNoMessage {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub committer: String,
    pub author: String,
}

#[derive(Debug, Default)]
pub struct CommitOnlyTreeAndParents {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
}

impl CommitOnlyTreeAndParents {
    pub fn parse(raw: &[u8]) -> io::Result<Self> {
        let mut curr_index = 0;
        CommitOnlyTreeAndParents::parse_inner(raw, &mut curr_index)
    }

    pub fn parse_inner(raw: &[u8], curr: &mut usize) -> io::Result<Self> {
        let mut out = Self::default();
        let (tree_id, next_index) = parse_tree(raw)?;
        out.tree = tree_id;
        *curr = next_index;

        // is there a parent?
        let parent_option = parse_parent(raw, curr)?;
        if let Some(parent) = parent_option {
            out.parent_one = parent;
        } else {
            return Ok(out);
        }

        // Yes, we found first parent, but what about second parent?
        let parent_option = parse_parent(raw, curr)?;
        if let Some(parent) = parent_option {
            out.parent_two = parent;
        } else {
            return Ok(out);
        }

        // now, we loop, and add a potentially arbitrary number of parents:
        loop {
            if let Some(parent) = parse_parent(raw, curr)? {
                out.extra_parents.push(parent);
            } else {
                // No extra parent, we are done parsing:
                break;
            }
        }

        Ok(out)
    }
}

pub fn parse_tree(raw: &[u8]) -> io::Result<(Oid, usize)> {
    // a tree line should be 5 bytes for the string "tree "
    // and then 40 bytes for the hex chars of the tree oid,
    // and then 1 byte as the newline. so lets get
    // 46 bytes to check if this line is valid:
    let line = raw.get(0..46).ok_or_else(|| ioerr!("First line not long enough to contain a tree id"))?;
    if &line[0..5] != b"tree " {
        return ioerre!("Expected first line of commit object to be 'tree '");
    }
    if line[45] != b'\n' {
        return ioerre!("Expected newline after tree id");
    }
    // at this point we are reasonably confident
    // that we have a valid tree...
    // remember, for the oid, we only want 32 chars,
    // 5 + 32 = 37:
    let oid_str = std::str::from_utf8(&line[5..37]).map_err(|e| ioerr!("{}", e))?;
    let oid = Oid::from_str_radix(oid_str, 16).map_err(|e| ioerr!("{}", e))?;
    let next_index_starts_at = 46;
    Ok((oid, next_index_starts_at))
}

pub fn parse_parent(raw: &[u8], curr_index: &mut usize) -> io::Result<Option<Oid>> {
    // a parent line should be 7 bytes for the string "parent "
    // and then 40 bytes for the hex chars of the tree oid,
    // and then 1 byte as the newline. so lets get
    // 48 bytes to check if this line is valid:
    // BUT, we need to check if this is a parent line, or the next line
    // is author, in which case we return Ok(None) because there is no parent
    // so get the first 7 chars and test if its author or parent:
    let start_index = *curr_index;
    let desired_range = start_index..(start_index + 7);
    let line = raw.get(desired_range)
        .ok_or_else(|| ioerr!("First line not long enough to contain a parent id"))?;
    
    if &line[0..7] == b"author " {
        // no need to advance the current index
        // because the caller will then use this index
        // to look for an author string
        return Ok(None);
    }
    
    // otherwise, we expect this to be a parent line
    if &line[0..7] != b"parent " {
        return ioerre!("Expected first line of commit object to be 'tree '");
    }
    // now, lets get the rest of the line, which should just be the hash
    // and a new line, so 40 + 1 chars:
    let desired_range = (start_index + 7)..(start_index + 7 + 41);
    let line = raw.get(desired_range)
        .ok_or_else(|| ioerr!("First line not long enough to contain a parent id"))?;

    if line[40] != b'\n' {
        return ioerre!("Expected newline after parent id");
    }
    // at this point we are reasonably confident
    // that we have a valid parent...
    // remember, we only want 32 chars for the hash:
    let oid_str = std::str::from_utf8(&line[0..32]).map_err(|e| ioerr!("{}", e))?;
    let oid = Oid::from_str_radix(oid_str, 16).map_err(|e| ioerr!("{}", e))?;
    let next_index_starts_at = start_index + 7 + 41;
    *curr_index = next_index_starts_at;
    Ok(Some(oid))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizeoftest() {
        let size = std::mem::size_of::<CommitOnlyTreeAndParents>();
        assert_eq!(size, 72);
    }

    #[test]
    fn parse_tree_line_works() {
        // our OIDs only take first 32 hex chars
        // for the hash:
        let line = b"tree 0000000000000000000000000000000f00000000\nauthor me <me> 1623986985 -0500";
        let (tree_hash, next_index) = parse_tree(line).unwrap();
        assert_eq!(tree_hash, 15);
        assert_eq!(next_index, 46);
    }

    #[test]
    fn tree_and_parent_parsing_works() {
        let line = b"tree 0000000000000000000000000000000100000000\nparent 0000000000000000000000000000000200000000\nparent 0000000000000000000000000000000300000000\nauthor me...";
        let (tree_hash, mut next_index) = parse_tree(line).unwrap();
        assert_eq!(tree_hash, 1);
        assert_eq!(next_index, 46);

        let first_parent = parse_parent(line, &mut next_index).unwrap();
        assert_eq!(first_parent, Some(2));
        let second_parent = parse_parent(line, &mut next_index).unwrap();
        assert_eq!(second_parent, Some(3));
        let third_parent = parse_parent(line, &mut next_index).unwrap();
        assert_eq!(third_parent, None);
    }

    #[test]
    fn commit_only_tree_and_parents_parsing_works() {
        let line = b"tree 0000000000000000000000000000000100000000\nparent 0000000000000000000000000000000200000000\nparent 0000000000000000000000000000000300000000\nparent 0000000000000000000000000000000400000000\nauthor me <me> 12321321321 -0000";
        let obj = CommitOnlyTreeAndParents::parse(line).unwrap();
        assert_eq!(obj.tree, 1);
        assert_eq!(obj.parent_one, 2);
        assert_eq!(obj.parent_two, 3);
        assert_eq!(obj.extra_parents.len(), 1);
        assert_eq!(obj.extra_parents[0], 4);
    }
}
