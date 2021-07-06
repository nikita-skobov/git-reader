
use std::{fmt::Display, convert::TryFrom, io};
use commit_object_parsing::ParseCommit;
use super::{UnparsedObject, UnparsedObjectType};

pub mod commit_object_parsing;
pub mod tree_object_parsing;
pub mod blob_object_parsing;

use tree_object_parsing::ParseTree;
use blob_object_parsing::ParseBlob;

/// A trait thats used to define how you want
/// your objects parsed. this lets you avoid
/// reading/parsing certain objects. Of course, this
/// could potentially make traversing your objects
/// impossible if, for example, you use an empty Tree parser.
/// However, this can be very useful and still useable if
/// you make some optimizations such as not parsing blobs
/// if your application does not require them, or not parsing
/// the commit message/author information of commits.
/// See sensible choices of parsing combinations below
/// (read the source file). Or otherwise, make your own combination by doing:
/// ```
/// use git_reader::object_database::loose;
/// use loose::blob_object_parsing;
/// use loose::tree_object_parsing;
/// use loose::commit_object_parsing;
/// use loose::ParseObject;
///
/// pub struct MyCustomParser {}
/// impl ParseObject for MyCustomParser {
///     // here you can choose which parsing types your application needs:
///     type Commit = commit_object_parsing::CommitFull;
///     type Blob = blob_object_parsing::BlobObjRaw;
///     type Tree = tree_object_parsing::TreeObject;
/// }
/// ```
pub trait ParseObject {
    type Commit: ParseCommit;
    type Blob: ParseBlob;
    type Tree: ParseTree;
}

/// parse commits/trees/blobs fully.
/// blobs are parsed as raw vec of bytes.
pub struct ParseEverything {}
impl ParseObject for ParseEverything {
    type Commit = commit_object_parsing::CommitFull;
    type Blob = blob_object_parsing::BlobObjRaw;
    type Tree = tree_object_parsing::TreeObject;
}

/// Same as `ParseEverything` but blobs are strings.
/// Note this will error if your blob is not a string...
/// `ParseEverythingBlobStringsLossy` is recommended instead, as
/// that variant will not error.
pub struct ParseEverythingBlobStrings {}
impl ParseObject for ParseEverythingBlobStrings {
    type Commit = commit_object_parsing::CommitFull;
    type Blob = blob_object_parsing::BlobObjStringOrError;
    type Tree = tree_object_parsing::TreeObject;
}

pub struct ParseEverythingBlobStringsLossy {}
impl ParseObject for ParseEverythingBlobStringsLossy {
    type Commit = commit_object_parsing::CommitFull;
    type Blob = blob_object_parsing::BlobObjStringLossy;
    type Tree = tree_object_parsing::TreeObject;
}

/// Parse commits/trees fully, but drop blobs.
/// useful for logging for example.
pub struct ParseEverythingButBlobs {}
impl ParseObject for ParseEverythingButBlobs {
    type Commit = commit_object_parsing::CommitFull;
    type Blob = blob_object_parsing::BlobObjectNone;
    type Tree = tree_object_parsing::TreeObject;
}

/// The fastest way to parse objects without breaking
/// traverse-ability. blobs get dropped.
/// for commit parsing, we only parse the tree and parents, ie:
/// we dont parse the author/committer/message
pub struct ParseBareMinimal {}
impl ParseObject for ParseBareMinimal {
    type Commit = commit_object_parsing::CommitOnlyTreeAndParents;
    type Blob = blob_object_parsing::BlobObjectNone;
    type Tree = tree_object_parsing::TreeObject;
}

/// TODO: care about tags?
#[derive(Debug)]
pub struct TagObject {

}

#[derive(Debug)]
pub enum ParsedObject<T: ParseObject> {
    Commit(T::Commit),
    Tree(T::Tree),
    Blob(T::Blob),
    Tag(TagObject),
}

impl<T: ParseObject> Display for ParsedObject<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsedObject::Commit(c) => write!(f, "{}", c),
            ParsedObject::Tree(t) => write!(f, "{}", t),
            ParsedObject::Blob(b) => write!(f, "{}", b),
            ParsedObject::Tag(_) => write!(f, "tags not impl yet"),
        }
    }
}

impl<T: ParseObject> TryFrom<UnparsedObject> for ParsedObject<T> {
    type Error = io::Error;

    fn try_from(unparsed: UnparsedObject) -> Result<Self, Self::Error> {
        let obj = match unparsed.object_type {
            UnparsedObjectType::Tree => {
                let tree_obj = T::Tree::parse(&unparsed.payload)?;
                ParsedObject::Tree(tree_obj)
            }
            UnparsedObjectType::Commit => {
                let commit_obj = T::Commit::parse(&unparsed.payload)?;
                ParsedObject::Commit(commit_obj)
            }
            UnparsedObjectType::Blob => {
                let blob_obj = T::Blob::parse(&unparsed.payload)?;
                ParsedObject::Blob(blob_obj)
            }
            UnparsedObjectType::Tag => {
                ParsedObject::Tag(TagObject {})
            }
        };
        Ok(obj)
    }
}
