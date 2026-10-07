# tinystoragedrivers-secrets

The contract is specified in
[`docs/specs/secret-storage.md`](../../docs/specs/secret-storage.md).

Encrypted secret storage for OpenHuman: one `SecretStore` port (`get`, `set`,
`delete`, `list`), with drivers for each place a secret can live. Desktop and
cloud builds share the port and the byte formats.

| Driver | Where the bytes live | Use |
| --- | --- | --- |
| `MemorySecrets` | process memory | tests |
| `EncryptedFileSecrets` | `secrets.enc`, one ChaCha20-Poly1305 blob | desktop, CLI |
| `DocumentSecrets` | `{"ciphertext": "enc2:…"}` per secret in any `DocumentStore` | cloud, multi-tenant |
| `KeyringSecrets` (feature `keyring`) | the OS credential store | desktop |

## Formats

Every format is OpenHuman's own, so existing data opens unchanged:

- **`enc2:` values.** `enc2:` + lowercase hex of `nonce(12) ‖ ciphertext ‖
  tag(16)`, ChaCha20-Poly1305, no associated data. Upper-case hex is accepted
  on read. See `crypto::{encrypt_enc2, decrypt_enc2}`.
- **`secrets.enc`.** The JSON object `{"<name>": "<value>"}` encrypted as one
  raw `nonce ‖ ciphertext ‖ tag` blob. Values are text, so the file and keyring
  drivers reject non-UTF-8 values.
- **Keys.** 32 bytes written as 64 hex characters: OpenHuman's `.secret_key`
  file (`load_or_create_key_file`) and its keychain master key (service
  `openhuman`, user `app:master_key`; `KeyringSecrets::load_or_create_key`).
- **Legacy `enc:`.** Repeating-key XOR. Readable through
  `crypto::decrypt_legacy_enc` for migration only; never written.

The crate's tests decrypt golden vectors that OpenHuman's own crypto code
produced, and the reverse direction (this crate writes, OpenHuman's code
reads) was checked with the same code.

## Keys per tenant

`DocumentSecrets` asks a `KeyProvider` for the data key of its handle's scope
on every call:

- `StaticKey`: one key everywhere (from the environment, or KMS-unwrapped).
- `DerivedKeys`: `HKDF-SHA256(ikm = master, info =
  "tinystoragedrivers-secrets/v1:" ‖ scope)`. Each tenant gets a distinct
  key, and a ciphertext copied between tenants does not decrypt.

## Operational notes

- `EncryptedFileSecrets` takes the same `secrets.enc.lock` advisory lock as
  OpenHuman for its read → modify → write cycle. It writes through a unique
  temp file (`0600`, synced) and renames it into place.
- A `secrets.enc` that does not decrypt or parse fails the call
  (`ErrorKind::Crypto` / `Serialization`) and is left untouched. It is never
  treated as empty or overwritten.
- `KeyringSecrets` cannot enumerate (`enumerable() == false`, `list` fails with
  `ErrorKind::Backend`). A locked store is `ErrorKind::Unavailable`.
  `load_or_create_key` creates a key only when the entry is genuinely absent,
  and serializes creation within the process. The OS stores have no
  create-if-absent, so `with_key_creation_lock(path)` serializes it across
  processes through a shared lock file.
- `load_or_create_key_file` writes and syncs a temp sibling, then publishes it
  with a hard link that fails if the key file exists. Readers never see a
  partial key, and racing creators adopt the winner's key.
- File and keyring calls run on the tokio blocking pool when a runtime is
  present.
- Error messages never contain key bytes, plaintext, or secret names.

## Tests

`testkit::secrets_conformance` (feature `testkit`) runs the same suite
against every driver. The keyring driver runs it over an in-memory
credential builder. The real OS store is only touched by
`live_keyring_conformance`, and only with `TSD_LIVE_KEYRING=1`.
