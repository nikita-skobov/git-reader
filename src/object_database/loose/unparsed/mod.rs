use std::{io, str::FromStr, path::PathBuf};
use crate::ioerre;

pub mod decode;
pub use decode::*;

/// A loose object is either unresolved, in which case
/// it points to a file: 00/xyzdadadebebe that contains
/// the actual object, and we can read that file, and then
/// turn this into a resolved loose object, which has
/// the data loaded into memory.
#[derive(Debug)]
pub enum PartiallyResolvedLooseObject {
    Unresolved(PathBuf),
    Resolved(UnparsedObject),
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum UnparsedObjectType {
    Tree,
    Blob,
    Commit,
    Tag,
}

impl FromStr for UnparsedObjectType {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let objtype = match s {
            "tree" => UnparsedObjectType::Tree,
            "tag" => UnparsedObjectType::Tag,
            "commit" => UnparsedObjectType::Commit,
            "blob" => UnparsedObjectType::Blob,
            _ => { return ioerre!("Failed to parse object type of '{}'", s); },
        };
        Ok(objtype)
    }
}

#[derive(Debug)]
pub struct UnparsedObject {
    pub object_type: UnparsedObjectType,
    pub payload: Vec<u8>,
}


#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    fn nonsense3() {
        let path = "../.git/objects/2d/f9f3d514bd85575bd848e7bfedc6375f414cd9";
        let raw_obj = read_raw_object(path, false).unwrap();
        std::fs::write("testfile1_2df9f3d.txt", &raw_obj.payload).unwrap();
    }
}
