//! Name encoding: safe alphabet, case-insensitive distinctness, length limits.

use super::*;

#[test]
fn keeps_the_safe_alphabet_and_escapes_the_rest() {
    assert_eq!(encode("local"), "local");
    assert_eq!(encode("a-b_c9"), "a-b_c9");
    assert_eq!(encode("A"), "%41");
    assert_eq!(encode("a/b.c"), "a%2Fb%2Ec");
    assert_eq!(encode(".."), "%2E%2E");
    assert_eq!(encode("é"), "%C3%A9");
}

#[test]
fn names_differing_only_in_case_stay_distinct_when_case_folded() {
    let upper = encode("Tasks").to_lowercase();
    let lower = encode("tasks").to_lowercase();
    assert_ne!(upper, lower);
}

#[test]
fn escapes_windows_device_names() {
    assert_eq!(encode("con"), "%63on");
    assert_eq!(encode("nul"), "%6Eul");
    assert_eq!(encode("com1"), "%63om1");
    assert_eq!(encode("lpt9"), "%6Cpt9");
    assert_eq!(encode("comx"), "comx");
    assert_eq!(encode("com12"), "com12");
    assert_eq!(encode("console"), "console");
}

#[test]
fn hashes_long_file_names() {
    let short = "a".repeat(MAX_PLAIN);
    assert_eq!(file_stem(&short), short);
    let long = "a".repeat(MAX_PLAIN + 1);
    let stem = file_stem(&long);
    assert!(stem.starts_with('~'));
    assert_eq!(stem.len(), 33);
    assert_ne!(stem, file_stem(&"b".repeat(MAX_PLAIN + 1)));
    // Escapes count toward the limit.
    assert!(file_stem(&"A".repeat(70)).starts_with('~'));
}

#[test]
fn splits_long_directory_names() {
    assert_eq!(dir_components("local"), PathBuf::from("local"));
    let long = "x".repeat(MAX_PLAIN * 2 + 5);
    let parts: Vec<_> = dir_components(&long)
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].len(), MAX_PLAIN);
    assert!(parts[1].starts_with('+') && parts[1].len() == MAX_PLAIN + 1);
    assert_eq!(parts[2], "+xxxxx");
}

#[test]
fn fnv_matches_the_reference_vectors() {
    assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a_128(b""), 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d);
    assert_eq!(fnv1a_128(b"a"), 0xd228_cb69_6f1a_8caf_7891_2b70_4e4a_8964);
}
