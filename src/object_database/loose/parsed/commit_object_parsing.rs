use crate::{ioerre, object_id::{Oid, hex_u128_to_str}, ioerr};
use std::{fmt::Display, io};

pub trait ParseCommit: Display {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized;

    fn parse(raw: &[u8]) -> io::Result<Self> where Self: Sized {
        let mut index = 0;
        Self::parse_inner(raw, &mut index)
    }
}

/// The reason we use `parent_one`, `parent_two`
/// and then have a seperate `extra_parents` vec
/// is to minimize heap allocations. Most commits will have
/// one or two parents, and then an empty `extra_parents` vec
/// will not need heap allocation, which should make
/// us a lot faster at the cost of about 30% more memory usage.
/// TODO: what other filtered versions do we want?
#[derive(Debug, Default)]
pub struct CommitFull {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub author: String,
    pub committer: String,
    pub message: String,
}

/// Unlike `CommitFull` this will actually parse the commit message
/// and commit summary seperately, where the CommitFull just includes
/// the entire message as one string.
pub struct CommitFullMessageAndDescription {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub author: String,
    pub committer: String,
    pub message: String,
    pub description: String,
}

/// Like `CommitFullMessageAndDescription` we still
/// seperate a message and description, but we don't
/// parse the description part. ie: we only
/// allocate for the message.
pub struct CommitFullOnlyMessage {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub author: String,
    pub committer: String,
    pub message: String,
}

/// Like `CommitFullOnlyMessage` but we don't parse the
/// author or committer text
pub struct CommitOnlyMessageNoAuthorOrCommitter {
    pub tree: Oid,
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub message: String,
}

#[derive(Default)]
pub struct CommitOnlyParents {
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
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

pub struct CommitOnlyParentsAndMessage {
    pub parent_one: Oid,
    pub parent_two: Oid,
    pub extra_parents: Vec<Oid>,
    pub message: String,
}

impl Display for CommitFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tree_id_str = hex_u128_to_str(self.tree);
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "tree {}\n{}\nauthor {}\ncommitter {}\n\n{}", tree_id_str, parent_str, self.author, self.committer, self.message)
    }
}

impl Display for CommitOnlyParents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "{}\n", parent_str)
    }
}

impl Display for CommitFullOnlyMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tree_id_str = hex_u128_to_str(self.tree);
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "tree {}\n{}\nauthor {}\ncommitter {}\n\n{}", tree_id_str, parent_str, self.author, self.committer, self.message)
    }
}

impl Display for CommitOnlyMessageNoAuthorOrCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tree_id_str = hex_u128_to_str(self.tree);
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "tree {}\n{}\n\n{}", tree_id_str, parent_str, self.message)
    }
}

impl Display for CommitOnlyParentsAndMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "{}\n\n{}", parent_str, self.message)
    }
}

impl Display for CommitFullMessageAndDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tree_id_str = hex_u128_to_str(self.tree);
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "tree {}\n{}\nauthor {}\ncommitter {}\n\n{}\n\n{}", tree_id_str, parent_str, self.author, self.committer, self.message, self.description)
    }
}

impl Display for CommitOnlyTreeAndParents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tree_id_str = hex_u128_to_str(self.tree);
        let parent_str = if self.parent_one == 0 {
            "".into()
        } else {
            format!("parent {}", hex_u128_to_str(self.parent_one))
        };
        let mut parent_str = if self.parent_two == 0 {
            parent_str
        } else {
            format!("{}\nparent {}", parent_str, hex_u128_to_str(self.parent_two))
        };
        for parent in self.extra_parents.iter() {
            parent_str = format!("{}\nparent {}", parent_str, hex_u128_to_str(*parent));
        }
        write!(f, "tree {}\n{}\n", tree_id_str, parent_str)
    }
}

impl ParseCommit for CommitFull {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let only_tree_and_parents = CommitOnlyTreeAndParents::parse_inner(raw, current_index)?;

        // the hard part is done, now we can just parse the committer/author
        // and message
        let author = parse_author(raw, current_index, true)?;
        let committer = parse_committer(raw, current_index, true)?;
        let rest_of_data = &raw[*current_index..];
        // the rest of the data should be the commit message.
        // we dont want trailing newlines though, so we do this:
        let mut last_index = rest_of_data.len() - 1;
        let mut last_char = rest_of_data[last_index];
        while last_char == b'\n' {
            last_index -= 1;
            last_char = *rest_of_data.get(last_index)
                .ok_or_else(|| ioerr!("Failed to trim newlines from commit message. Does your commit message consist entirely of new lines?"))?;
        }
        let commit_message_raw = &rest_of_data[0..last_index + 1];
        let message = String::from_utf8_lossy(commit_message_raw);

        let obj = CommitFull {
            tree: only_tree_and_parents.tree,
            parent_one: only_tree_and_parents.parent_one,
            parent_two: only_tree_and_parents.parent_two,
            extra_parents: only_tree_and_parents.extra_parents,
            author,
            committer,
            message: message.into(),
        };
        Ok(obj)
    }
}

impl ParseCommit for CommitFullOnlyMessage {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let only_tree_and_parents = CommitOnlyTreeAndParents::parse_inner(raw, current_index)?;

        // the hard part is done, now we can just parse the committer/author
        // and message
        let author = parse_author(raw, current_index, true)?;
        let committer = parse_committer(raw, current_index, true)?;
        let rest_of_data = &raw[*current_index..];
        // for the only message mode, we wish to only allocate for the
        // first part of the commit message, so we read up to
        // the first newline we find. if we don't find the newline, then
        // we take everything:
        let message = if let Some(newline_index) = rest_of_data.iter().position(|b| *b == b'\n') {
            let commit_message_raw = &rest_of_data[0..newline_index];
            String::from_utf8_lossy(commit_message_raw)
        } else {
            let commit_message_raw = &rest_of_data[0..];
            String::from_utf8_lossy(commit_message_raw)
        };

        let obj = CommitFullOnlyMessage {
            tree: only_tree_and_parents.tree,
            parent_one: only_tree_and_parents.parent_one,
            parent_two: only_tree_and_parents.parent_two,
            extra_parents: only_tree_and_parents.extra_parents,
            author,
            committer,
            message: message.into(),
        };
        Ok(obj)
    }
}

impl ParseCommit for CommitOnlyMessageNoAuthorOrCommitter {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let only_tree_and_parents = CommitOnlyTreeAndParents::parse_inner(raw, current_index)?;

        // the hard part is done, now we can just parse the committer/author
        // and message
        let _ = parse_author(raw, current_index, false)?;
        let _ = parse_committer(raw, current_index, false)?;
        let rest_of_data = &raw[*current_index..];
        // for the only message mode, we wish to only allocate for the
        // first part of the commit message, so we read up to
        // the first newline we find. if we don't find the newline, then
        // we take everything:
        let message = if let Some(newline_index) = rest_of_data.iter().position(|b| *b == b'\n') {
            let commit_message_raw = &rest_of_data[0..newline_index];
            String::from_utf8_lossy(commit_message_raw)
        } else {
            let commit_message_raw = &rest_of_data[0..];
            String::from_utf8_lossy(commit_message_raw)
        };

        let obj = CommitOnlyMessageNoAuthorOrCommitter {
            tree: only_tree_and_parents.tree,
            parent_one: only_tree_and_parents.parent_one,
            parent_two: only_tree_and_parents.parent_two,
            extra_parents: only_tree_and_parents.extra_parents,
            message: message.into(),
        };
        Ok(obj)
    }
}

impl ParseCommit for CommitOnlyParentsAndMessage {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let only_parents = CommitOnlyParents::parse_inner(raw, current_index)?;
        let _ = parse_author(raw, current_index, false)?;
        let _ = parse_committer(raw, current_index, false)?;
        let rest_of_data = &raw[*current_index..];
        // for the only message mode, we wish to only allocate for the
        // first part of the commit message, so we read up to
        // the first newline we find. if we don't find the newline, then
        // we take everything:
        let message = if let Some(newline_index) = rest_of_data.iter().position(|b| *b == b'\n') {
            let commit_message_raw = &rest_of_data[0..newline_index];
            String::from_utf8_lossy(commit_message_raw)
        } else {
            let commit_message_raw = &rest_of_data[0..];
            String::from_utf8_lossy(commit_message_raw)
        };
        // TODO: can we parse merge tags faster?
        let obj = Self {
            parent_one: only_parents.parent_one,
            parent_two: only_parents.parent_two,
            extra_parents: only_parents.extra_parents,
            message: message.to_string(),
        };
        Ok(obj)
    }
}

impl ParseCommit for CommitFullMessageAndDescription {
    fn parse_inner(
        raw: &[u8],
        current_index: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let full_commit = CommitFull::parse_inner(raw, current_index)?;
        // now from the full commit we can just parse out the
        // commit message/description by checking if theres 2 newlines
        // in the message:
        let new_obj = if let Some(newline_index) = full_commit.message.find("\n\n") {
            // if we found a newline index then
            // we have a message and a description:
            let message = &full_commit.message[0..newline_index];
            let description = &full_commit.message[(newline_index + 1)..];
            CommitFullMessageAndDescription {
                message: message.into(),
                description: description.into(),
                tree: full_commit.tree,
                parent_one: full_commit.parent_one,
                parent_two: full_commit.parent_two,
                extra_parents: full_commit.extra_parents,
                author: full_commit.author,
                committer: full_commit.committer,
            }
        } else {
            // otherwise its just a message, and there is no description:
            CommitFullMessageAndDescription {
                tree: full_commit.tree,
                parent_one: full_commit.parent_one,
                parent_two: full_commit.parent_two,
                extra_parents: full_commit.extra_parents,
                author: full_commit.author,
                committer: full_commit.committer,
                message: full_commit.message,
                description: String::with_capacity(0),
            }
        };
        Ok(new_obj)
    }
}

impl ParseCommit for CommitOnlyTreeAndParents {
    fn parse_inner(
        raw: &[u8],
        curr: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let mut out = Self::default();
        let (tree_id, next_index) = parse_tree(raw, true)?;
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

impl ParseCommit for CommitOnlyParents {
    fn parse_inner(
        raw: &[u8],
        curr: &mut usize
    ) -> io::Result<Self> where Self: Sized {
        let mut out = Self::default();
        let (_, next_index) = parse_tree(raw, false)?;
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

/// If should allocate is false, we dont actually create a string.
/// This is useful for when you want to only advance the `curr_index` but
/// you don't care about the author string
pub fn parse_author(
    raw: &[u8],
    curr_index: &mut usize,
    should_allocate: bool,
) -> io::Result<String> {
    let start_index = *curr_index;
    let desired_range = start_index..(start_index + 7);
    let line = raw.get(desired_range)
        .ok_or_else(|| ioerr!("First line not long enough to contain author string"))?;
    if &line[0..7] != b"author " {
        return ioerre!("Expected first line of author line to contain 'author'");
    }
    // this means we know the author string starts at index
    // start_index + 7, so next, we find the nearest newline:
    let rest_of_data = &raw[(start_index + 7)..];
    let newline_index = rest_of_data.iter().position(|&b| b == b'\n')
        .ok_or_else(|| ioerr!("Failed to find newline when parsing author line"))?;

    let author_line = &rest_of_data[0..newline_index];
    let author_str = if should_allocate {
        String::from_utf8_lossy(author_line).into()
    } else {
        String::with_capacity(0)
    };
    *curr_index = start_index + 7 + newline_index + 1;
    Ok(author_str)
}

/// this function is a misnomer. its purpose is to SKIP the mergetag string
/// for now, I do not care about the merge tag's significance, but
/// in the future maybe mergetag parsing will be necessary. for now we just
/// skip over it if we detect it:
pub fn parse_mergetag(
    raw: &[u8],
    curr_index: &mut usize
) -> io::Result<()> {
    let start_index = *curr_index;
    let desired_range = start_index..(start_index + 9);
    let line = raw.get(desired_range)
        .ok_or_else(|| ioerr!("First line not long enough to contain mergetag string"))?;
    if &line[0..9] != b"mergetag " {
        return ioerre!("Expected first line of mergetag line to contain 'mergetag'");
    }
    // for now we dont do any merge tag parsing, we just want to
    // advance the current index to the end of the merge tag.
    let current_data = &raw[start_index..];
    let mut skip_chars = 0;
    let mut last_char_was_newline = false;
    for byte in current_data {
        if last_char_was_newline {
            // our last char was a newline. if our current char
            // is a space then we are still parsing the mergetag object
            if *byte == b' ' {
                last_char_was_newline = false;
                skip_chars += 1;
            } else if *byte == b'\n' {
                // this indicates we are done parsing the mergetag
                // data.
                // still need to skip 1 char because this is a newline
                // and we dont want the message parsing to start
                // on a newline
                skip_chars += 1;
                break;
            } else {
                // this is possibly another merge tag?
                // lets set our current index to the current skip_chars
                // and recurse:
                *curr_index += skip_chars;
                parse_mergetag(raw, curr_index)?;
                return Ok(());
            }
        } else {
            // we arent immediately after a newline, so
            // just increment the skip chars count
            // and continue
            skip_chars += 1;

            // but we also have to check if our CURRENT
            // char is a newline, if so we set
            // last_char_was_newline for next iteration to check
            last_char_was_newline = *byte == b'\n';
        }
    }
    *curr_index += skip_chars;
    Ok(())
}

/// If should allocate is false, we dont actually create a string.
/// This is useful for when you want to only advance the `curr_index` but
/// you don't care about the author string
pub fn parse_committer(
    raw: &[u8],
    curr_index: &mut usize,
    should_allocate: bool,
) -> io::Result<String> {
    let start_index = *curr_index;
    let desired_range = start_index..(start_index + 10);
    let line = raw.get(desired_range)
        .ok_or_else(|| ioerr!("First line not long enough to contain committer string"))?;
    if &line[0..10] != b"committer " {
        return ioerre!("Expected first line of committer line to contain 'committer'");
    }
    // this means we know the committer string starts at index
    // start_index + 10, so next, we find the nearest newline:
    let rest_of_data = &raw[(start_index + 10)..];
    let newline_index = rest_of_data.iter().position(|&b| b == b'\n')
        .ok_or_else(|| ioerr!("Failed to find newline when parsing committer line"))?;

    let committer_line = &rest_of_data[0..newline_index];
    let committer_str = if should_allocate {
        String::from_utf8_lossy(committer_line).into()
    } else {
        String::with_capacity(0)
    };

    // at the end of the committer line, there should be 2 newlines.
    // we verify that here. If there is not 2 newlines, then
    // this should be a mergetag object
    if rest_of_data[newline_index + 1] != b'\n' {
        // we add 1 here to skip the one newline that we DID find above,
        // so now the current index should point to the beginning of the merge
        // tag object.
        *curr_index = start_index + 10 + newline_index + 1;
        // for now we dont implement actually parsing the
        // merge tag, instead we just want to advance the current index
        // past it.
        parse_mergetag(raw, curr_index)?;
    } else {
        // if we did find 2 trailing newlines, we add 2
        *curr_index = start_index + 10 + newline_index + 2;
    }
    Ok(committer_str)
}

pub fn parse_tree(
    raw: &[u8],
    should_allocate: bool,
) -> io::Result<(Oid, usize)> {
    if !should_allocate {
        // this just assumes a tree is here, and skips
        // to index 46 (past the tree line)
        return Ok((0, 46));
    }
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
        let (tree_hash, next_index) = parse_tree(line, true).unwrap();
        assert_eq!(tree_hash, 15);
        assert_eq!(next_index, 46);
    }

    #[test]
    fn tree_and_parent_parsing_works() {
        let line = b"tree 0000000000000000000000000000000100000000\nparent 0000000000000000000000000000000200000000\nparent 0000000000000000000000000000000300000000\nauthor me...";
        let (tree_hash, mut next_index) = parse_tree(line, true).unwrap();
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
