
use std::{fmt::Display, path::PathBuf, convert::TryFrom, io};
use commit_object_parsing::ParseCommit;
use super::{read_raw_object, Resolve};
use super::{UnparsedObject, UnparsedObjectType};
use crate::ioerre;

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

pub enum PartiallyParsedLooseObject<T: ParseCommit> {
    Unresolved(PathBuf),
    Parsed(ParsedObject<T>)
}

impl<T: ParseCommit> Resolve for PartiallyParsedLooseObject<T> {
    type Object = ParsedObject<T>;

    fn unresolved(p: PathBuf) -> Self {
        PartiallyParsedLooseObject::Unresolved(p)
    }

    fn resolve_or_return(&mut self) -> std::io::Result<Option<&Self::Object>> {
        match self {
            PartiallyParsedLooseObject::Parsed(obj) => Ok(Some(obj)),
            PartiallyParsedLooseObject::Unresolved(path) => {
                let unparsed = read_raw_object(path, false)?;
                let obj = Self::make_object_from_unparsed(unparsed)?;
                *self = PartiallyParsedLooseObject::Parsed(obj);
                match self {
                    PartiallyParsedLooseObject::Parsed(obj) => Ok(Some(obj)),
                    _ => return ioerre!("Failed to insert resolved object"),
                }
            }
        }
    }

    fn return_if_resolved(&self) -> std::io::Result<Option<&Self::Object>> {
        match self {
            PartiallyParsedLooseObject::Parsed(obj) => Ok(Some(obj)),
            PartiallyParsedLooseObject::Unresolved(_) => Ok(None)
        }
    }

    fn make_object_from_unparsed(unparsed: super::UnparsedObject) -> std::io::Result<Self::Object> {
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