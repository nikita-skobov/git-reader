use std::{io, str::FromStr, path::PathBuf};
use crate::ioerre;
use super::Resolve;

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

impl Resolve for PartiallyResolvedLooseObject {
    type Object = UnparsedObject;

    fn unresolved(p: PathBuf) -> Self {
        PartiallyResolvedLooseObject::Unresolved(p)
    }

    fn resolve_or_return(&mut self) -> io::Result<Option<&Self::Object>> {
        match self {
            PartiallyResolvedLooseObject::Resolved(object_type) => Ok(Some(object_type)),
            PartiallyResolvedLooseObject::Unresolved(path) => {
                let resolved_obj = read_raw_object(path, false)?;
                *self = PartiallyResolvedLooseObject::Resolved(resolved_obj);
                match self {
                    PartiallyResolvedLooseObject::Resolved(object_type) => Ok(Some(object_type)),
                    _ => return ioerre!("Failed to insert resolved object"),
                }
            }
        }
    }

    fn return_if_resolved(&self) -> io::Result<Option<&Self::Object>> {
        match self {
            PartiallyResolvedLooseObject::Resolved(object_type) => Ok(Some(object_type)),
            PartiallyResolvedLooseObject::Unresolved(path) => Ok(None),
        }
    }

    fn make_object_from_unparsed(unparsed: UnparsedObject) -> io::Result<Self::Object> {
        Ok(unparsed)
    }
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
