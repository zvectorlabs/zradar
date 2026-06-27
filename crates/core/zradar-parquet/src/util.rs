//! Small utility helpers shared across the Parquet crate.
//!
//! Foundation for Phase 0/1 of the NeMo compatibility work
//! (see `zradar-plans/nemo-compatibility/techspec/TECH-SPEC-PHASE-0.md` §4.5).

use anyhow::{Result, anyhow};

/// Safely quote a SQL identifier (column or table name) for use in raw SQL
/// strings, enforcing the D-G2 allowlist.
///
/// Per `DECISIONS.md` D-G2, identifiers must match the allowlist pattern
/// `[a-zA-Z][a-zA-Z0-9_]*` (optionally dot-separated for qualified names such
/// as `schema.table.column`). Anything that does not match — `;`, `'`, `--`,
/// `/*`, spaces, leading digits, unicode, embedded double-quotes, empty
/// segments — is rejected outright. The output is wrapped in `"..."` for
/// SQL.
///
/// Because the input is restricted to an injection-free allowlist, the output
/// never needs to escape characters inside the quotes; the surrounding `"`
/// pair is purely conventional.
///
/// # Errors
///
/// Returns an error if `name` is empty, contains an empty dot segment, starts
/// with a digit, or contains any character outside `[a-zA-Z0-9_.]`.
///
/// # Examples
///
/// ```
/// use zradar_parquet::util::quote_identifier;
///
/// assert_eq!(quote_identifier("rail_type").unwrap(), "\"rail_type\"");
/// assert_eq!(
///     quote_identifier("gen_ai.request.model").unwrap(),
///     "\"gen_ai.request.model\""
/// );
/// assert!(quote_identifier("1bad").is_err());
/// assert!(quote_identifier("bad;drop").is_err());
/// assert!(quote_identifier("").is_err());
/// ```
pub fn quote_identifier(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(anyhow!("quote_identifier: identifier must not be empty"));
    }

    // Reject obvious injection sequences explicitly for a clearer error.
    // The allowlist below already rejects each character individually, but
    // these checks produce better error messages and make intent obvious.
    for bad in [";", "'", "--", "/*", "*/"] {
        if name.contains(bad) {
            return Err(anyhow!(
                "quote_identifier: identifier {:?} contains forbidden sequence {:?}",
                name,
                bad
            ));
        }
    }

    // Validate each dot-separated segment against [a-zA-Z][a-zA-Z0-9_]*.
    for segment in name.split('.') {
        if segment.is_empty() {
            return Err(anyhow!(
                "quote_identifier: identifier {:?} has empty segment",
                name
            ));
        }
        let mut chars = segment.chars();
        let first = chars.next().unwrap();
        if !first.is_ascii_alphabetic() {
            return Err(anyhow!(
                "quote_identifier: identifier {:?} segment must start with [a-zA-Z], got {:?}",
                name,
                first
            ));
        }
        for ch in chars {
            if !(ch.is_ascii_alphanumeric() || ch == '_') {
                return Err(anyhow!(
                    "quote_identifier: identifier {:?} contains disallowed character {:?}",
                    name,
                    ch
                ));
            }
        }
    }

    // Safe to wrap directly: the allowlist forbids `"` and other special chars.
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    out.push_str(name);
    out.push('"');
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier_rejects_empty() {
        let err = quote_identifier("").expect_err("empty input must error");
        assert!(format!("{err}").contains("empty"));
    }

    #[test]
    fn test_quote_identifier_accepts_simple_name() {
        assert_eq!(quote_identifier("rail_type").unwrap(), "\"rail_type\"");
        assert_eq!(quote_identifier("a").unwrap(), "\"a\"");
        assert_eq!(quote_identifier("A").unwrap(), "\"A\"");
        assert_eq!(quote_identifier("agent_name").unwrap(), "\"agent_name\"");
    }

    #[test]
    fn test_quote_identifier_accepts_dotted_name() {
        assert_eq!(
            quote_identifier("gen_ai.request.model").unwrap(),
            "\"gen_ai.request.model\""
        );
        assert_eq!(quote_identifier("a.b").unwrap(), "\"a.b\"");
    }

    #[test]
    fn test_quote_identifier_rejects_leading_digit() {
        assert!(quote_identifier("1bad").is_err());
        assert!(quote_identifier("123").is_err());
        // First segment must start with letter even in dotted names.
        assert!(quote_identifier("1a.b").is_err());
        // Second segment must also start with a letter.
        assert!(quote_identifier("a.1b").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_injection_sequences() {
        assert!(quote_identifier("bad;drop").is_err());
        assert!(quote_identifier("a';--").is_err());
        assert!(quote_identifier("a/*b*/c").is_err());
        assert!(quote_identifier("a--b").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_spaces() {
        assert!(quote_identifier("with space").is_err());
        assert!(quote_identifier(" leading").is_err());
        assert!(quote_identifier("trailing ").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_quotes() {
        assert!(quote_identifier("a\"b").is_err());
        assert!(quote_identifier("\"").is_err());
        assert!(quote_identifier("'sneaky'").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_unicode() {
        assert!(quote_identifier("café").is_err());
        assert!(quote_identifier("naïve").is_err());
        assert!(quote_identifier("a😀b").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_backslash() {
        assert!(quote_identifier("a\\b").is_err());
        assert!(quote_identifier("path\\with\\sep").is_err());
    }

    #[test]
    fn test_quote_identifier_rejects_empty_segments() {
        assert!(quote_identifier(".").is_err());
        assert!(quote_identifier("a..b").is_err());
        assert!(quote_identifier(".a").is_err());
        assert!(quote_identifier("a.").is_err());
    }
}
