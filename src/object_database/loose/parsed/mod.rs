
use std::{fmt::Display, convert::TryFrom, io};
use commit_object_parsing::ParseCommit;
use super::{UnparsedObject, UnparsedObjectType};

pub mod commit_object_parsing;
pub mod tree_object_parsing;

use tree_object_parsing::TreeObject;

/// TODO: do we ever want to load
/// an entire blob into memory?
#[derive(Debug)]
pub struct BlobObject {

}

/// TODO: care about tags?
#[derive(Debug)]
pub struct TagObject {

}

#[derive(Debug)]
pub enum ParsedObject<T: ParseCommit> {
    Commit(T),
    Tree(TreeObject),
    Blob(BlobObject),
    Tag(TagObject),
}

impl<T: ParseCommit> Display for ParsedObject<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsedObject::Commit(c) => write!(f, "{}", c),
            ParsedObject::Tree(t) => write!(f, "{}", t),
            ParsedObject::Blob(_) => write!(f, "blobs not impl yet"),
            ParsedObject::Tag(_) => write!(f, "tags not impl yet"),
        }
    }
}

impl<T: ParseCommit> TryFrom<UnparsedObject> for ParsedObject<T> {
    type Error = io::Error;

    fn try_from(unparsed: UnparsedObject) -> Result<Self, Self::Error> {
        let obj = match unparsed.object_type {
            UnparsedObjectType::Tree => {
                let tree_obj = TreeObject::parse(&unparsed.payload)?;
                ParsedObject::Tree(tree_obj)
            }
            UnparsedObjectType::Commit => {
                let commit_obj = T::parse(&unparsed.payload)?;
                ParsedObject::Commit(commit_obj)
            }
            UnparsedObjectType::Blob => {
                ParsedObject::Blob(BlobObject {})
            }
            UnparsedObjectType::Tag => {
                ParsedObject::Tag(TagObject {})
            }
        };
        Ok(obj)
    }
}
