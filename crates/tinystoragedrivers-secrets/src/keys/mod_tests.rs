//! Static and derived key providers.

use tinystoragedrivers_core::ErrorKind;

use super::*;
use crate::crypto::key_to_hex;

const MASTER_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

#[test]
fn a_static_key_is_the_same_for_every_scope() {
    let provider = StaticKey::from_hex(MASTER_HEX).unwrap();
    let a = provider.data_key(&Scope::local()).unwrap();
    let b = provider.data_key(&Scope::new("tenant-b").unwrap()).unwrap();
    assert_eq!(*a, *b);
    assert_eq!(key_to_hex(&a).as_str(), MASTER_HEX);
}

#[test]
fn derived_keys_differ_per_scope_and_from_the_master() {
    let provider = DerivedKeys::from_hex(MASTER_HEX).unwrap();
    let a = provider.data_key(&Scope::new("tenant-a").unwrap()).unwrap();
    let b = provider.data_key(&Scope::new("tenant-b").unwrap()).unwrap();
    assert_ne!(*a, *b);
    assert_ne!(key_to_hex(&a).as_str(), MASTER_HEX);
    // Deterministic: the same scope always gets the same key.
    let again = provider.data_key(&Scope::new("tenant-a").unwrap()).unwrap();
    assert_eq!(*a, *again);
}

#[test]
fn derivation_is_pinned_to_hkdf_sha256_with_the_v1_label() {
    // Pinned so a refactor cannot silently re-key every tenant.
    let provider = DerivedKeys::from_hex(MASTER_HEX).unwrap();
    let key = provider.data_key(&Scope::local()).unwrap();
    let hkdf = Hkdf::<Sha256>::new(None, &key_from_hex(MASTER_HEX).unwrap()[..]);
    let mut expected = [0u8; KEY_LEN];
    hkdf.expand(b"tinystoragedrivers-secrets/v1:local", &mut expected)
        .unwrap();
    assert_eq!(*key, expected);
}

#[test]
fn bad_hex_is_a_crypto_error() {
    assert_eq!(
        StaticKey::from_hex("abc").unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        DerivedKeys::from_hex("00").unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn debug_never_prints_key_material() {
    let static_key = StaticKey::from_hex(MASTER_HEX).unwrap();
    let derived = DerivedKeys::from_hex(MASTER_HEX).unwrap();
    assert_eq!(format!("{static_key:?}"), "StaticKey(..)");
    assert_eq!(format!("{derived:?}"), "DerivedKeys(..)");
}
