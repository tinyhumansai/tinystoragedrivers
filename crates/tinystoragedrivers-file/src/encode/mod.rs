//! Turning arbitrary names into file and directory names.
//!
//! Scopes, collection names, document ids, stream names and blob keys reach
//! this driver as arbitrary UTF-8, but a path component must avoid `/`, `\`,
//! `.`/`..`, Windows device names, and the 255-byte limit most filesystems
//! impose, and must stay distinct on case-insensitive filesystems (the default
//! on macOS and Windows).
//!
//! [`encode`] keeps lowercase ASCII letters, digits, `_` and `-` and writes every
//! other byte as `%XX` with uppercase hex. Its output never contains a
//! lowercase hex digit after `%`, so two different names can never encode to
//! strings that differ only in case, and it never contains `.`, so a suffix
//! such as `.json` or `.meta.json` is unambiguous.
//!
//! Two ways keep the result under the component limit:
//!
//! - [`file_stem`] (documents, streams, blobs, collection specs) replaces an
//!   encoding longer than [`MAX_PLAIN`] with `~` and a 128-bit FNV-1a hash. The
//!   real name is always stored inside the file, so a reader verifies it and
//!   listings recover it.
//! - [`dir_components`] (scopes, collections) splits a long encoding into
//!   chunks, continuing with `+`-prefixed directories. Nothing is hashed, so two
//!   scopes can never share a directory.

use std::path::PathBuf;

/// The longest encoded name used as-is. Leaves room for `.meta.json` and the
/// temporary-file prefix inside a 255-byte component.
pub(crate) const MAX_PLAIN: usize = 200;

/// Windows device names, which are unusable as a file name with any
/// extension.
const RESERVED: [&str; 4] = ["con", "prn", "aux", "nul"];

/// Percent-encode `name` into the filesystem-safe alphabet.
pub(crate) fn encode(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for byte in name.bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-') {
            out.push(char::from(byte));
        } else {
            push_escape(&mut out, byte);
        }
    }
    if is_reserved(&out) {
        // Escape the first character; still a valid, unique encoding.
        let mut escaped = String::with_capacity(out.len() + 2);
        push_escape(&mut escaped, out.as_bytes()[0]);
        escaped.push_str(&out[1..]);
        return escaped;
    }
    out
}

fn push_escape(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(char::from(HEX[usize::from(byte >> 4)]));
    out.push(char::from(HEX[usize::from(byte & 0x0f)]));
}

/// Whether `encoded` is a Windows device name (`con`, `com1`, `lpt9`, ...).
fn is_reserved(encoded: &str) -> bool {
    if RESERVED.contains(&encoded) {
        return true;
    }
    let bytes = encoded.as_bytes();
    bytes.len() == 4
        && (encoded.starts_with("com") || encoded.starts_with("lpt"))
        && bytes[3].is_ascii_digit()
}

/// The file stem for a stored name: its encoding, or a hash of the name when
/// the encoding is too long.
pub(crate) fn file_stem(name: &str) -> String {
    let encoded = encode(name);
    if encoded.len() <= MAX_PLAIN {
        encoded
    } else {
        format!("~{:032x}", fnv1a_128(name.as_bytes()))
    }
}

/// The directory components for a scope or collection: the encoding split into
/// [`MAX_PLAIN`]-byte chunks, every chunk after the first prefixed with `+`.
pub(crate) fn dir_components(name: &str) -> PathBuf {
    let encoded = encode(name);
    let mut path = PathBuf::new();
    // The encoding is ASCII, so any byte offset is a char boundary.
    let mut rest = encoded.as_str();
    let mut first = true;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(rest.len().min(MAX_PLAIN));
        if first {
            path.push(chunk);
            first = false;
        } else {
            path.push(format!("+{chunk}"));
        }
        rest = tail;
    }
    path
}

/// 64-bit FNV-1a: stable across processes and releases, unlike the standard
/// library's hasher, so cursors that embed it stay valid after a restart.
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

/// 128-bit FNV-1a, for file names derived from long ids.
pub(crate) fn fnv1a_128(bytes: &[u8]) -> u128 {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u128::from(*byte)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
