//! The tenant boundary every stored record carries.
//!
//! A [`Scope`] partitions one backend between independent owners: on a
//! multi-tenant MongoDB deployment it is the user (agent) a record belongs to,
//! and on a single-operator desktop it is [`Scope::local`]. Port handles are
//! opened *for* a scope (see
//! [`StorageBackend::for_scope`](crate::StorageBackend::for_scope)), so no port
//! method takes one and no call site can forget it.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Result, StorageError};

/// The longest scope a driver must be able to store, in bytes.
pub const MAX_SCOPE_LEN: usize = 256;

/// A validated tenant key.
///
/// A scope is 1 to [`MAX_SCOPE_LEN`] bytes of printable text without
/// whitespace, so it can be stored as a plain column or field and appear in a
/// log line without escaping.
///
/// ```
/// use tinystoragedrivers_core::Scope;
///
/// let scope = Scope::new("agent:alice")?;
/// assert_eq!(scope.as_str(), "agent:alice");
/// assert!(Scope::new("").is_err());
/// assert!(Scope::local().is_local());
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scope(Arc<str>);

impl Scope {
    /// The value of [`Scope::local`].
    pub const LOCAL: &'static str = "local";

    /// Validate `value` as a scope.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// value is empty, longer than [`MAX_SCOPE_LEN`] bytes, or contains
    /// whitespace or control characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(StorageError::invalid_input("scope must not be empty"));
        }
        if value.len() > MAX_SCOPE_LEN {
            return Err(StorageError::invalid_input(format!(
                "scope is {} bytes, longer than the {MAX_SCOPE_LEN} byte limit",
                value.len()
            )));
        }
        if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(StorageError::invalid_input(
                "scope must not contain whitespace or control characters",
            ));
        }
        Ok(Self(Arc::from(value)))
    }

    /// The scope a single-operator installation stores everything under.
    #[must_use]
    pub fn local() -> Self {
        Self(Arc::from(Self::LOCAL))
    }

    /// Whether this is [`Scope::local`].
    #[must_use]
    pub fn is_local(&self) -> bool {
        &*self.0 == Self::LOCAL
    }

    /// The scope as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scope({:?})", &*self.0)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Scope {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Serialize for Scope {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
