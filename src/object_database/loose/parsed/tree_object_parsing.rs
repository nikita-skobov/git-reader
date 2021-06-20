
use crate::{ioerr, object_id::{OidTruncated, Oid, trunc_oid_to_u128_oid, hex_u128_to_str}, ioerre};
use std::{convert::TryFrom, io, fmt::Display};


/// See:
/// https://stackoverflow.com/a/8347325
#[derive(Debug, PartialOrd, PartialEq)]
pub enum TreeMode {
    /// 040000
    Directory,
    /// 100644
    RegularNonEx,
    /// 100664
    RegularNonExGroupWrite,
    /// 100755
    RegularEx,
    /// 120000
    SymLink,
    /// 160000
    GitLink,
}

impl TreeMode {
    pub fn is_blob(&self) -> bool {
        match self {
            TreeMode::Directory => false,
            TreeMode::RegularNonEx |
            TreeMode::RegularNonExGroupWrite |
            TreeMode::RegularEx |
            TreeMode::SymLink => {
                true
            }
            // TODO: get rid of panic once you find out...
            TreeMode::GitLink => {
                panic!("I DONT KNOW IF A GITLINK IS A BLOB OR NOT");
            }
        }
    }
}

impl AsRef<str> for TreeMode {
    fn as_ref(&self) -> &str {
        match self {
            TreeMode::Directory => "040000",
            TreeMode::RegularNonEx => "100644",
            TreeMode::RegularNonExGroupWrite => "100664",
            TreeMode::RegularEx => "100755",
            TreeMode::SymLink => "120000",
            TreeMode::GitLink => "160000",
        }
    }
}

impl Default for TreeMode {
    fn default() -> Self {
        TreeMode::Directory
    }
}

impl TryFrom<&[u8]> for TreeMode {
    type Error = io::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let out = match value {
            b"40000" => TreeMode::Directory,
            b"100644" => TreeMode::RegularNonEx,
            b"100664" => TreeMode::RegularNonExGroupWrite,
            b"100640" => TreeMode::RegularNonEx,
            b"100755" => TreeMode::RegularEx,
            b"120000" => TreeMode::SymLink,
            b"160000" => TreeMode::GitLink,
            _ => return ioerre!("Failed to find appropriate tree mode for: {:?}", value),
        };
        Ok(out)
    }
}

#[derive(Debug, Default)]
pub struct TreeEntry {
    pub id: Oid,
    pub path_component: String,
    pub entry_mode: TreeMode,
}

#[derive(Debug, Default)]
pub struct TreeObject {
    pub entries: Vec<TreeEntry>,
}

impl ToString for TreeEntry {
    fn to_string(&self) -> String {
        let mode_str = self.entry_mode.as_ref();
        let blob_or_tree = if self.entry_mode.is_blob() {
            "blob"
        } else {
            "tree"
        };
        let id_str = hex_u128_to_str(self.id);
        format!("{} {} {}\t{}", mode_str, blob_or_tree, id_str, self.path_component)
    }
}

impl Display for TreeObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entry_str = self.entries.iter().map(|e| e.to_string()).collect::<Vec<String>>()
            .join("\n");
        write!(f, "{}", entry_str)
    }
}

pub fn get_tree_entry(raw: &[u8], curr: &mut usize) -> io::Result<TreeEntry> {
    // get everything up to the null byte:
    let raw = &raw[*curr..];
    let null_byte_index = raw.iter().position(|&b| b == 0)
        .ok_or_else(|| ioerr!("Failed to parse tree entry: no null byte detected"))?;
    let string_part = &raw[0..null_byte_index];
    let space_index = string_part.iter().position(|&b| b == b' ')
        .ok_or_else(|| ioerr!("Failed to parse tree entry: no space found to seperate mode from file component"))?;
    let mode = &string_part[0..space_index];
    let tree_mode = TreeMode::try_from(mode)?;
    let path_component = &string_part[(space_index + 1)..];
    let path_component = std::str::from_utf8(path_component)
        .map_err(|e| ioerr!("Failed to parse path component: {}", e))?;
    // dont heap allocate the string until
    // we verify that the hash is valid:
    let desired_range = (null_byte_index + 1)..(null_byte_index + 1 + 20);
    let last_segment = raw.get(desired_range)
        .ok_or_else(|| ioerr!("Failed to find sha hash of tree entry"))?;
    // we got the whole 20 byte hex slice,
    // but remember we only care about the first 16 to make an Oid:
    let mut oid = OidTruncated::default();
    oid[..].copy_from_slice(&last_segment[0..16]);
    let oid = trunc_oid_to_u128_oid(oid);
    
    // if we got this far, we successfully parsed this entry,
    // so adjust the current index:
    let this_entry_len = null_byte_index + 1 + 20;
    *curr = *curr + this_entry_len;
    let tree_entry = TreeEntry {
        id: oid,
        entry_mode: tree_mode,
        path_component: path_component.to_owned(),
    };

    Ok(tree_entry)
}

impl TreeObject {
    pub fn parse(raw: &[u8]) -> io::Result<TreeObject> {
        let mut index = 0;
        let raw_len = raw.len();
        let mut object = TreeObject::default();
        while index < raw_len {
            let entry = get_tree_entry(raw, &mut index)?;
            object.entries.push(entry);
        }

        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_id::OidFull;

    #[test]
    fn tree_parse_works() {
        let mut oid_full_1 = OidFull::default();
        oid_full_1[15] = 1;
        let mut oid_full_2 = OidFull::default();
        oid_full_2[15] = 2;
        let mut tree_vec = b"40000 dir1\0".to_vec();
        tree_vec.extend(&oid_full_1);
        tree_vec.extend(b"100644 somefile\0");
        tree_vec.extend(&oid_full_2);

        let parsed = TreeObject::parse(&tree_vec[..]).unwrap();
        assert_eq!(parsed.entries.len(), 2);
        let first_entry = &parsed.entries[0];
        let second_entry = &parsed.entries[1];
        assert_eq!(first_entry.id, 1);
        assert_eq!(second_entry.id, 2);
        assert_eq!(first_entry.path_component, "dir1");
        assert_eq!(second_entry.path_component, "somefile");
        assert_eq!(first_entry.entry_mode, TreeMode::Directory);
        assert_eq!(second_entry.entry_mode, TreeMode::RegularNonEx);
    }

    #[test]
    fn size_test() {
        let size = std::mem::size_of::<TreeMode>();
        assert_eq!(size, 1);
        let size = std::mem::size_of::<TreeEntry>();
        assert_eq!(size, 48);
        let size = std::mem::size_of::<TreeObject>();
        assert_eq!(size, 24);
    }
}
