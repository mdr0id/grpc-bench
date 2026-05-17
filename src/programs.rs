//! Loader for the `--programs <path>` TSV file (spec §3).
//!
//! Format: `short_name<TAB>program_id<TAB>description` per row. Blank lines
//! and lines beginning with `#` are skipped. All other rows are loaded into a
//! single subscription filter set (spec §4 — one filter, many owners).
//!
//! `program_id` must be a valid Solana base58 pubkey (32 bytes decoded).
//! Malformed rows are reported with the source path and 1-indexed line number.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use serde::Serialize;
use thiserror::Error;

/// One program entry from the TSV file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProgramEntry {
    /// Short identifier used in per-program t-digest keys (spec §6.2).
    pub short_name: String,
    /// Solana program ID in base58 form. Always exactly 32 bytes decoded.
    pub program_id: String,
    /// Free-text description; surfaced verbatim in the output JSON `programs`
    /// block (the output JSON schema). Empty string if the source row had only two columns.
    pub description: String,
}

/// Parsed `--programs` TSV file.
#[derive(Debug, Clone, Serialize)]
pub struct ProgramSet {
    /// Source path used to load the set, for diagnostics.
    pub source: PathBuf,
    /// Programs in load order. Order is preserved from the file so that the
    /// summary JSON's `programs` field round-trips identically.
    pub entries: Vec<ProgramEntry>,
}

/// Errors emitted by [`ProgramSet::load`].
#[derive(Debug, Error)]
pub enum ProgramsError {
    /// The TSV file could not be opened.
    #[error("failed to open programs file {path}: {source}")]
    Open {
        /// Path that failed to open.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// An I/O error occurred while reading the TSV.
    #[error("failed to read programs file {path}: {source}")]
    Read {
        /// Path that failed mid-read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// A non-comment, non-blank row could not be parsed.
    #[error("{path}:{line}: malformed row: {reason}")]
    MalformedRow {
        /// Source TSV file path.
        path: PathBuf,
        /// 1-indexed line number in the TSV.
        line: usize,
        /// Human-readable reason for the parse failure.
        reason: String,
    },
    /// The TSV contained zero valid program entries.
    #[error("{path}: file contained no program entries")]
    Empty {
        /// Source TSV file path.
        path: PathBuf,
    },
    /// Two rows shared the same `short_name`; the per-program bucket key
    /// must be unique within the file.
    #[error("{path}:{line}: duplicate short_name {short_name:?}")]
    DuplicateShortName {
        /// Source TSV file path.
        path: PathBuf,
        /// 1-indexed line number of the duplicate row.
        line: usize,
        /// The conflicting short name.
        short_name: String,
    },
    /// Two rows shared the same `program_id`. We refuse to silently dedupe
    /// because that would mask a typo in one of the entries.
    #[error("{path}:{line}: duplicate program_id {program_id}")]
    DuplicateProgramId {
        /// Source TSV file path.
        path: PathBuf,
        /// 1-indexed line number of the duplicate row.
        line: usize,
        /// The conflicting program ID.
        program_id: String,
    },
}

impl ProgramSet {
    /// Load and validate a programs TSV from disk.
    ///
    /// # Errors
    /// Returns [`ProgramsError`] for I/O failures, malformed rows, duplicate
    /// names or program IDs, or an entirely empty set after stripping
    /// comments and blank lines.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProgramsError> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref).map_err(|source| ProgramsError::Open {
            path: path_ref.to_path_buf(),
            source,
        })?;
        let reader = BufReader::new(file);
        Self::from_reader(path_ref.to_path_buf(), reader)
    }

    /// Variant of [`Self::load`] that reads from an arbitrary buffered
    /// reader. Used by tests.
    ///
    /// # Errors
    /// Same conditions as [`Self::load`] minus the file-open failure.
    pub fn from_reader<R: BufRead>(
        source: PathBuf,
        reader: R,
    ) -> Result<Self, ProgramsError> {
        let mut entries: Vec<ProgramEntry> = Vec::new();
        for (idx, line) in reader.lines().enumerate() {
            let line_no = idx + 1;
            let raw = line.map_err(|source_err| ProgramsError::Read {
                path: source.clone(),
                source: source_err,
            })?;
            let trimmed = raw.trim_end_matches(['\r', '\n']);
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            let entry = parse_row(&source, line_no, trimmed)?;

            if let Some(dup) = entries.iter().find(|e| e.short_name == entry.short_name) {
                return Err(ProgramsError::DuplicateShortName {
                    path: source,
                    line: line_no,
                    short_name: dup.short_name.clone(),
                });
            }
            if let Some(dup) = entries.iter().find(|e| e.program_id == entry.program_id) {
                return Err(ProgramsError::DuplicateProgramId {
                    path: source,
                    line: line_no,
                    program_id: dup.program_id.clone(),
                });
            }

            entries.push(entry);
        }

        if entries.is_empty() {
            return Err(ProgramsError::Empty { path: source });
        }

        Ok(Self { source, entries })
    }

    /// Return all program IDs in load order as base58 strings. This is the
    /// list fed into `accounts.filters[].owner` and
    /// `transactions.filters[].account_include` (spec §4).
    #[must_use]
    pub fn program_ids(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.program_id.clone()).collect()
    }

    /// Number of program entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the program set is empty. Always `false` for a successfully
    /// loaded set (load fails on empty input), but the method is kept for
    /// API ergonomics and to satisfy clippy's `len_without_is_empty` lint.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn parse_row(path: &Path, line_no: usize, raw: &str) -> Result<ProgramEntry, ProgramsError> {
    let mut parts = raw.split('\t');
    let short_name = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ProgramsError::MalformedRow {
            path: path.to_path_buf(),
            line: line_no,
            reason: "missing short_name (first tab-separated column)".to_string(),
        })?;
    let program_id_raw = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ProgramsError::MalformedRow {
            path: path.to_path_buf(),
            line: line_no,
            reason: "missing program_id (second tab-separated column)".to_string(),
        })?;
    let description = parts.next().map_or(String::new(), |s| s.trim().to_string());
    // Reject any trailing column past description — that indicates a
    // malformed row (stray tab) rather than benign trailing whitespace.
    if parts.next().is_some() {
        return Err(ProgramsError::MalformedRow {
            path: path.to_path_buf(),
            line: line_no,
            reason: "more than three tab-separated columns".to_string(),
        });
    }

    validate_program_id(program_id_raw).map_err(|reason| ProgramsError::MalformedRow {
        path: path.to_path_buf(),
        line: line_no,
        reason,
    })?;

    Ok(ProgramEntry {
        short_name: short_name.to_string(),
        program_id: program_id_raw.to_string(),
        description,
    })
}

fn validate_program_id(s: &str) -> Result<(), String> {
    let decoded = bs58::decode(s)
        .into_vec()
        .map_err(|e| format!("program_id {s:?} is not valid base58: {e}"))?;
    if decoded.len() != 32 {
        return Err(format!(
            "program_id {s:?} decoded to {} bytes, want 32",
            decoded.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    fn p() -> PathBuf {
        PathBuf::from("test.tsv")
    }

    #[test]
    fn parses_three_column_rows() {
        let src = "raydium_amm_v4\t675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\tRaydium AMM v4\n\
                   spl_token\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\tSPL Token\n";
        let set = ProgramSet::from_reader(p(), Cursor::new(src)).expect("parse");
        assert_eq!(set.len(), 2);
        assert_eq!(set.entries[0].short_name, "raydium_amm_v4");
        assert_eq!(set.entries[1].program_id, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(set.entries[0].description, "Raydium AMM v4");
    }

    #[test]
    fn allows_missing_description() {
        let src = "system\t11111111111111111111111111111111\n";
        let set = ProgramSet::from_reader(p(), Cursor::new(src)).expect("parse");
        assert_eq!(set.entries[0].description, "");
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let src = "# header comment\n\
                   \n\
                   spl_token\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\tSPL Token\n\
                   # trailing comment\n";
        let set = ProgramSet::from_reader(p(), Cursor::new(src)).expect("parse");
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn rejects_missing_short_name() {
        let src = "\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\tSPL Token\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::MalformedRow { .. }));
    }

    #[test]
    fn rejects_missing_program_id() {
        let src = "spl_token\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::MalformedRow { .. }));
    }

    #[test]
    fn rejects_invalid_base58() {
        let src = "bad\t!!!not-base58!!!\tdesc\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::MalformedRow { .. }));
    }

    #[test]
    fn rejects_short_pubkey() {
        // valid base58 but only 5 bytes decoded
        let src = "short\tabcde\tdesc\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::MalformedRow { .. }));
    }

    #[test]
    fn rejects_duplicate_short_name() {
        let src = "spl_token\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\n\
                   spl_token\t11111111111111111111111111111111\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::DuplicateShortName { .. }));
    }

    #[test]
    fn rejects_duplicate_program_id() {
        let src = "a\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\n\
                   b\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::DuplicateProgramId { .. }));
    }

    #[test]
    fn rejects_empty_file() {
        let src = "# only comments\n\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::Empty { .. }));
    }

    #[test]
    fn rejects_four_column_row() {
        let src = "a\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\tdesc\textra\n";
        let err = ProgramSet::from_reader(p(), Cursor::new(src)).unwrap_err();
        assert!(matches!(err, ProgramsError::MalformedRow { .. }));
    }

    #[test]
    fn program_ids_preserves_order() {
        let src = "a\t11111111111111111111111111111111\n\
                   b\tTokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\n";
        let set = ProgramSet::from_reader(p(), Cursor::new(src)).expect("parse");
        assert_eq!(
            set.program_ids(),
            vec![
                "11111111111111111111111111111111".to_string(),
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            ]
        );
    }
}
