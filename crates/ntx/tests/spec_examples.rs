//! Checks the parser against the examples in the spec repository.
//!
//! The spec is found at `$NTX_SPEC_DIR`, or next to this repository (`../spec`).

use std::fs;
use std::path::PathBuf;

use ntx::protocol::Message;

fn spec_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("NTX_SPEC_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../spec"),
        PathBuf::from,
    );
    dir.join("examples").is_dir().then_some(dir)
}

fn examples(kind: &str) -> Vec<(String, String)> {
    let Some(dir) = spec_dir() else {
        assert!(
            std::env::var_os("CI").is_none(),
            "spec repository not found; set NTX_SPEC_DIR"
        );
        eprintln!("spec repository not found; skipping");
        return vec![];
    };
    let mut files: Vec<_> = fs::read_dir(dir.join("examples").join(kind))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read_to_string(&p).unwrap(),
            )
        })
        .collect()
}

#[test]
fn valid_examples_parse() {
    for (name, text) in examples("valid") {
        let parsed = Message::parse(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
        if name == "unknown-op.json" {
            assert!(parsed.is_none(), "{name}: unknown ops must be ignored");
        } else {
            let message = parsed.unwrap_or_else(|| panic!("{name}: not recognised"));
            // Round trip: our JSON must parse back to the same message.
            assert_eq!(
                Message::parse(&message.to_json()).unwrap(),
                Some(message),
                "{name}"
            );
        }
    }
}

#[test]
fn invalid_examples_are_rejected() {
    for (name, text) in examples("invalid") {
        assert!(Message::parse(&text).is_err(), "{name} should be rejected");
    }
}

#[test]
fn known_type_descriptors_are_valid() {
    let Some(dir) = spec_dir() else { return };
    for entry in fs::read_dir(dir.join("types")).unwrap() {
        let path = entry.unwrap().path();
        if path.to_string_lossy().ends_with(".ntxtype.json") {
            let descriptor: ntx::TypeDescriptor =
                serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            descriptor
                .validate()
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        }
    }
}
