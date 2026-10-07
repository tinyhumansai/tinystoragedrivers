//! Where a data key comes from: [`KeyProvider`] and its two implementations.
//!
//! [`DocumentSecrets`](crate::DocumentSecrets) asks its provider for the key of
//! the scope it is bound to on every operation, so a provider can hand each
//! tenant its own key. Neither implementation here caches anything beyond the
//! material it was built with, and neither prints it in `Debug`.

use std::fmt;

use hkdf::Hkdf;
use sha2::Sha256;
use tinystoragedrivers_core::{Result, Scope, StorageError};
use zeroize::Zeroizing;

use crate::crypto::{KEY_LEN, key_from_hex};

/// Hands out the 32-byte data key for a scope.
pub trait KeyProvider: Send + Sync + fmt::Debug {
    /// The key that encrypts `scope`'s secrets.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) when
    /// no key can be produced, or whatever the provider's key source reports
    /// (a KMS outage would be [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable)).
    fn data_key(&self, scope: &Scope) -> Result<Zeroizing<[u8; KEY_LEN]>>;
}

/// One key for every scope: key material read from the environment, or
/// unwrapped from a KMS at boot.
pub struct StaticKey {
    key: Zeroizing<[u8; KEY_LEN]>,
}

impl StaticKey {
    /// Use `key` for every scope.
    #[must_use]
    pub fn new(key: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self { key }
    }

    /// Parse the key from hex (see [`crypto::key_from_hex`](crate::crypto::key_from_hex)).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) for
    /// malformed hex or a wrong length.
    pub fn from_hex(hex: &str) -> Result<Self> {
        key_from_hex(hex).map(Self::new)
    }
}

impl fmt::Debug for StaticKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StaticKey(..)")
    }
}

impl KeyProvider for StaticKey {
    fn data_key(&self, _scope: &Scope) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        Ok(self.key.clone())
    }
}

/// A distinct key per scope, derived from one master key with HKDF-SHA256.
///
/// The derivation is `HKDF-SHA256(salt = none, ikm = master, info =
/// "tinystoragedrivers-secrets/v1:" ‖ scope)`, expanded to 32 bytes. The
/// version label keeps a future scheme from colliding with this one, and the
/// scope as `info` means a leaked tenant key exposes no other tenant and says
/// nothing about the master.
pub struct DerivedKeys {
    master: Zeroizing<[u8; KEY_LEN]>,
}

/// The `info` prefix of every derivation; changing it re-keys every tenant.
const DERIVATION_LABEL: &[u8] = b"tinystoragedrivers-secrets/v1:";

impl DerivedKeys {
    /// Derive scope keys from `master`.
    #[must_use]
    pub fn new(master: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self { master }
    }

    /// Parse the master key from hex.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) for
    /// malformed hex or a wrong length.
    pub fn from_hex(hex: &str) -> Result<Self> {
        key_from_hex(hex).map(Self::new)
    }
}

impl fmt::Debug for DerivedKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DerivedKeys(..)")
    }
}

impl KeyProvider for DerivedKeys {
    fn data_key(&self, scope: &Scope) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        let hkdf = Hkdf::<Sha256>::new(None, self.master.as_slice());
        let mut key = Zeroizing::new([0u8; KEY_LEN]);
        let info = [DERIVATION_LABEL, scope.as_str().as_bytes()];
        hkdf.expand_multi_info(&info, &mut key[..])
            .map_err(|_| StorageError::crypto("key derivation failed"))?;
        Ok(key)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
