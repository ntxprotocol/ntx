//! Text rules (spec clause 17).

use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

/// Applies the text rules: NFC normalisation and `\n` line breaks (clauses 17.2, 17.3).
#[must_use]
pub fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n").nfc().collect()
}

/// The longest start of `text` whose `size` is at most `max`, where `size` adds up over pieces
/// (bytes or characters). It does not split a grapheme cluster, such as a letter and its
/// combining marks, unless the first cluster alone is too long; it never splits a character
/// (clause 6.2).
pub(crate) fn shorten(text: &str, max: usize, size: fn(&str) -> usize) -> &str {
    let fit = |pieces: &mut dyn Iterator<Item = (usize, &str)>| {
        let mut used = 0;
        let mut end = 0;
        for (start, piece) in pieces {
            used += size(piece);
            if used > max {
                break;
            }
            end = start + piece.len();
        }
        end
    };
    let mut end = fit(&mut text.grapheme_indices(true));
    if end == 0 {
        end = fit(&mut text.char_indices().map(|(i, c)| (i, &text[i..i + c.len_utf8()])));
    }
    &text[..end]
}

/// The number of characters (Unicode scalar values) in `text`.
pub(crate) fn chars(text: &str) -> usize {
    text.chars().count()
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

    #[test]
    fn shortening_keeps_clusters_whole() {
        // "ọ́" is 1 + 2 + 2 bytes and 3 characters as one cluster.
        let yoruba = "o\u{0323}\u{0301}";
        assert_eq!(shorten(&yoruba.repeat(3), 12, str::len), yoruba.repeat(2));
        assert_eq!(shorten(&yoruba.repeat(3), 7, chars), yoruba.repeat(2));
        assert_eq!(shorten("short", 63, str::len), "short");
        // A single cluster longer than the limit is cut between characters.
        let marks = format!("a{}", "\u{0301}".repeat(40));
        assert_eq!(shorten(&marks, 63, str::len), &marks[..63]);
        assert_eq!(shorten("", 5, str::len), "");
    }
}
