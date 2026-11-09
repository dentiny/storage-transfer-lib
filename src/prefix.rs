use crate::{Error, Result};

/// Validate a bucket-relative directory prefix and add its trailing slash.
/// An empty prefix selects the bucket root. Prefixes are literal object-key
/// segments, not URLs: percent escapes are never decoded.
pub fn normalize_directory_prefix(prefix: &str) -> Result<String> {
    if prefix.is_empty() {
        return Ok(String::new());
    }
    let path = prefix.strip_suffix('/').unwrap_or(prefix);
    if prefix.len() > 1024
        || prefix.trim() != prefix
        || prefix.chars().any(char::is_control)
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(Error::Unsupported(
            "directory prefix must be a canonical relative path, or empty for the bucket root"
                .into(),
        ));
    }
    let normalized = format!("{path}/");
    if normalized.len() > 1024 {
        return Err(Error::Unsupported("directory prefix is too long".into()));
    }
    Ok(normalized)
}

pub(crate) fn validate_root(root: &str) -> Result<()> {
    let Some(prefix) = root.strip_prefix('/') else {
        return Err(Error::InvalidRoot);
    };
    if !normalize_directory_prefix(prefix).is_ok_and(|normalized| normalized == prefix) {
        return Err(Error::InvalidRoot);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_directory_prefixes_without_changing_key_segments() {
        for (input, expected) in [
            ("", ""),
            ("raw", "raw/"),
            ("raw/", "raw/"),
            ("nested/深い directory/%2F", "nested/深い directory/%2F/"),
        ] {
            assert_eq!(normalize_directory_prefix(input).unwrap(), expected);
        }
        for invalid in [
            "/",
            "/raw",
            "raw//",
            "raw//nested",
            ".",
            "..",
            "raw/../x",
            "raw/./x",
            " raw",
            "raw\n",
        ] {
            assert!(normalize_directory_prefix(invalid).is_err(), "{invalid:?}");
        }
        assert!(normalize_directory_prefix(&"a".repeat(1024)).is_err());
    }
}
