//! The CHANGELOG's API list must match the code.
//!
//! The published list drifted badly once: it advertised Metadata v12,
//! Fetch v13 and OffsetFetch v8 while `SUPPORTED_VERSIONS` implemented
//! v0–v9, v4–v11 and v0–v1. Fifteen of twenty entries were wrong, so the
//! single most-read description of what this broker speaks was fiction —
//! and nothing failed.
//!
//! `src/server/versions.rs` already keeps `SUPPORTED_VERSIONS` (what
//! `ApiVersions` advertises) locked to `PARSER_ENCODER_COVERAGE` (what the
//! wire code implements) via a unit test. This file extends that chain one
//! link further out, to the documentation, so the three can't disagree.

use kafkaesque::server::versions::SUPPORTED_VERSIONS;

/// Parse the `### Supported Kafka APIs` bullet list out of CHANGELOG.md.
///
/// Accepts both `- Name (v3)` and `- Name (v3–v9)`, with either an ASCII
/// hyphen or an en dash as the range separator.
fn parse_changelog_api_list(markdown: &str) -> Vec<(String, i16, i16)> {
    let section = markdown
        .split("### Supported Kafka APIs")
        .nth(1)
        .expect("CHANGELOG.md must have a `### Supported Kafka APIs` section");
    // The list ends at the next heading.
    let section = section.split("\n#").next().unwrap_or(section);

    let mut out = Vec::new();
    for line in section.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("- ") else {
            continue;
        };
        let Some((name, versions)) = rest.split_once(" (v") else {
            continue;
        };
        let Some(versions) = versions.strip_suffix(')') else {
            continue;
        };
        // `3` (single) or `3–v9` / `3-v9` (range).
        let (lo, hi) = match versions.split_once(['–', '-']) {
            Some((lo, hi)) => (lo, hi.trim_start_matches('v')),
            None => (versions, versions),
        };
        let lo: i16 = lo
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("bad min version in CHANGELOG line {line:?}: {e}"));
        let hi: i16 = hi
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("bad max version in CHANGELOG line {line:?}: {e}"));
        out.push((name.trim().to_string(), lo, hi));
    }
    out
}

#[test]
fn changelog_api_versions_match_supported_versions() {
    let markdown = include_str!("../CHANGELOG.md");
    let documented = parse_changelog_api_list(markdown);

    assert!(
        !documented.is_empty(),
        "failed to parse any API entries out of the CHANGELOG's supported-APIs list"
    );

    // Every documented entry must match the advertised range exactly.
    for (name, lo, hi) in &documented {
        let actual = SUPPORTED_VERSIONS
            .iter()
            .find(|sv| sv.api_key.as_str() == name.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "CHANGELOG documents `{name}` but SUPPORTED_VERSIONS has no such API. \
                     Either the API was removed or the name is misspelled."
                )
            });
        assert_eq!(
            (*lo, *hi),
            (actual.min_version, actual.max_version),
            "CHANGELOG claims {name} v{lo}–v{hi} but the broker implements and \
             advertises v{}–v{}. Update CHANGELOG.md from src/server/versions.rs.",
            actual.min_version,
            actual.max_version,
        );
    }

    // And every advertised API must be documented — a silently-added API is
    // the same drift in the other direction.
    for sv in SUPPORTED_VERSIONS {
        assert!(
            documented.iter().any(|(n, _, _)| n == sv.api_key.as_str()),
            "{} is advertised via ApiVersions but missing from the CHANGELOG's \
             supported-APIs list",
            sv.api_key.as_str(),
        );
    }

    assert_eq!(
        documented.len(),
        SUPPORTED_VERSIONS.len(),
        "CHANGELOG lists {} APIs, SUPPORTED_VERSIONS has {} — the lists must \
         correspond one-to-one (duplicate CHANGELOG entry?)",
        documented.len(),
        SUPPORTED_VERSIONS.len(),
    );
}

/// The docs must not claim production readiness for a broker whose own
/// gap-pin tests document missing multi-node coverage. This pins the wording
/// so the claim can't quietly come back.
#[test]
fn lib_docs_do_not_claim_production_readiness() {
    let lib = include_str!("../src/lib.rs");
    assert!(
        !lib.contains("production-ready") && !lib.contains("production ready"),
        "src/lib.rs claims production readiness. Multi-node HA has no test \
         harness (see tests/p2_multinode_gap_pins_tests.rs) and replication is \
         not implemented, so the claim is not supportable."
    );
}
