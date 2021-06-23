use std::{fmt::Display, io};
use crate::ioerr;

pub trait ParseBlob: Display {
    fn parse(raw: &[u8]) -> io::Result<Self> where Self: Sized;
}

/// Dont parse blobs. Safest/Fastest.
pub struct BlobObjectNone {

}

/// Second Safest/fastest way to parse a blob object.
/// just take ownership of its raw data.
pub struct BlobObjRaw {
    pub raw: Vec<u8>,
}

/// Parse blobs by trying to do
/// `String::from_utf8(raw)`, which of course
/// can error, and if so your parsing breaks. It's fast,
/// but should only be used if you somehow can guarantee your
/// blob does not contain invalid utf8... which is difficult.
/// Its recommended to use `BlobObjStringLossy` instead.
pub struct BlobObjStringOrError {
    pub s: String,
}

/// Parse blobs by doing `String::from_utf8_lossy(raw)`
/// This is useful when you want to guarantee that you can view your blob
/// as a string, without errors. If your blob happened to be binary
/// or otherwise contain invalid utf8, you will see weird
/// symbols, but it will not error.
pub struct BlobObjStringLossy {
    pub s: String,
}

impl Display for BlobObjectNone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blob")
    }
}

impl Display for BlobObjRaw {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.raw)
    }
}

impl Display for BlobObjStringLossy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

impl Display for BlobObjStringOrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

impl ParseBlob for BlobObjectNone {
    fn parse(_raw: &[u8]) -> io::Result<Self> where Self: Sized {
        Ok(BlobObjectNone {})
    }
}

impl ParseBlob for BlobObjRaw {
    fn parse(raw: &[u8]) -> io::Result<Self> where Self: Sized {
        Ok(BlobObjRaw { raw: raw.to_vec() })
    }
}

impl ParseBlob for BlobObjStringLossy {
    fn parse(raw: &[u8]) -> io::Result<Self> where Self: Sized {
        let cow_str = String::from_utf8_lossy(raw);
        Ok(BlobObjStringLossy { s: cow_str.to_string() })
    }
}

impl ParseBlob for BlobObjStringOrError {
    fn parse(raw: &[u8]) -> io::Result<Self> where Self: Sized {
        let raw_str = std::str::from_utf8(raw)
            .map_err(|e| ioerr!("Failed to convert raw blob data into a UTF8 string\n{}", e))?;
        Ok(BlobObjStringOrError { s: raw_str.to_owned() })
    }
}
