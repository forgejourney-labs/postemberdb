//! Public PostemberDB release identity.
//!
//! SemVer is the compatibility and package version. The release series and
//! codename are human-facing labels and do not imply a SemVer stability level.

/// Cargo package version used by Rust tooling and dependency resolution.
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Human-facing PostemberDB release series. This is independent of SemVer.
pub const RELEASE_SERIES: u32 = 1;

/// Codename for the first PostemberDB release series.
pub const RELEASE_CODENAME: &str = "Ember";

/// Returns the full human-readable product version.
pub fn version_string() -> String {
    format!(
        "PostemberDB {ENGINE_VERSION} (series {RELEASE_SERIES}: {RELEASE_CODENAME})"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_identity_is_distinct_from_semver_package_version() {
        assert!(!ENGINE_VERSION.is_empty());
        assert_eq!(RELEASE_SERIES, 1);
        assert_eq!(RELEASE_CODENAME, "Ember");
        assert_eq!(
            version_string(),
            format!("PostemberDB {ENGINE_VERSION} (series 1: Ember)")
        );
    }
}
