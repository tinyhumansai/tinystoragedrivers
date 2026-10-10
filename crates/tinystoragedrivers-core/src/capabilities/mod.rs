//! What a driver can do beyond the baseline every driver must implement.
//!
//! The baseline is the whole [`DocumentStore`](crate::DocumentStore),
//! [`StreamStore`](crate::StreamStore) and [`BlobStore`](crate::BlobStore)
//! surface except the operations named here. A repository that needs one of
//! these checks [`Capabilities::require`] when it opens, so a deployment on the
//! wrong backend fails at boot rather than on the first request that needs it.

use std::fmt;

use crate::error::{Result, StorageError};

/// One optional ability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// [`DocumentStore::search`](crate::DocumentStore::search) over the
    /// fields a collection's [`SearchSpec`](crate::SearchSpec) names.
    FullText,
    /// [`DocumentStore::atomic_batch`](crate::DocumentStore::atomic_batch):
    /// several writes across documents and collections that land together or
    /// not at all.
    Transactions,
    /// Documents expire on their collection's
    /// [`ttl_field`](crate::CollectionSpec::ttl_field).
    Ttl,
    /// [`StorageBackend::for_scope_fenced`](crate::StorageBackend::for_scope_fenced):
    /// handles whose writes the driver checks against a
    /// [`Fence`](crate::Fence) in the same atomic step.
    Fencing,
}

impl Capability {
    const ALL: [Self; 4] = [Self::FullText, Self::Transactions, Self::Ttl, Self::Fencing];

    const fn bit(self) -> u32 {
        match self {
            Self::FullText => 1,
            Self::Transactions => 1 << 1,
            Self::Ttl => 1 << 2,
            Self::Fencing => 1 << 3,
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::FullText => "full_text",
            Self::Transactions => "transactions",
            Self::Ttl => "ttl",
            Self::Fencing => "fencing",
        })
    }
}

/// A set of [`Capability`] values.
///
/// ```
/// use tinystoragedrivers_core::{Capabilities, Capability};
///
/// let caps = Capabilities::none().with(Capability::Ttl);
/// assert!(caps.contains(Capability::Ttl));
/// assert!(caps.require(Capability::FullText).is_err());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Capabilities(u32);

impl Capabilities {
    /// The empty set: a baseline driver.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Every capability this version of the crate defines.
    ///
    /// The set grows in later releases, so a backend should list the
    /// capabilities it actually has rather than report `all()`. In
    /// particular, a backend that reports [`Capability::Fencing`] must
    /// implement (or forward)
    /// [`StorageBackend::for_scope_fenced`](crate::StorageBackend::for_scope_fenced);
    /// the trait's default refuses with `Unsupported(Fencing)`.
    #[must_use]
    pub const fn all() -> Self {
        Self(
            Capability::FullText.bit()
                | Capability::Transactions.bit()
                | Capability::Ttl.bit()
                | Capability::Fencing.bit(),
        )
    }

    /// This set plus `capability`.
    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }

    /// This set minus `capability`.
    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self(self.0 & !capability.bit())
    }

    /// Whether `capability` is in the set.
    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    /// Succeed when `capability` is in the set.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) naming the
    /// missing capability.
    pub fn require(self, capability: Capability) -> Result<()> {
        if self.contains(capability) {
            Ok(())
        } else {
            Err(StorageError::unsupported(
                capability,
                format!("this storage driver does not provide {capability}"),
            ))
        }
    }

    /// The capabilities in the set, in a stable order.
    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |capability| self.contains(*capability))
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
