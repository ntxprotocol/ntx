//! Text rules (spec clause 17).

use unicode_normalization::UnicodeNormalization;

/// Applies the text rules: NFC normalisation and `\n` line breaks (clauses 17.2, 17.3).
#[must_use]
pub fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n").nfc().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_breaks() {
        assert_eq!(normalize("a\r\nb\rc\nd"), "a\nb\nc\nd");
    }

    #[test]
    fn nfc() {
        // "ọ́" written as o + combining dot below + combining acute.
        let decomposed = "o\u{0323}\u{0301}";
        assert_eq!(normalize(decomposed), "\u{1ECD}\u{0301}");
    }
}
