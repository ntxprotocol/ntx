//! Turning fields into display text (spec clause 16).
//!
//! ```
//! use ntx::binding::Pattern;
//! let fields = serde_json::json!({"reference": "John 3:16", "version": "KJV"});
//! let pattern = Pattern::parse("{reference} · {version}").unwrap();
//! assert_eq!(pattern.render(fields.as_object().unwrap()), "John 3:16 · KJV");
//! ```

use serde_json::Value;

use crate::{Error, Fields, Result};

/// Finds the value at a dot-separated field path such as `verses.0.text` (clause 16.2).
#[must_use]
pub fn lookup<'a>(fields: &'a Fields, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut value = fields.get(parts.next()?)?;
    for part in parts {
        value = match value {
            Value::Object(map) => map.get(part)?,
            Value::Array(items) => items.get(parse_index(part)?)?,
            _ => return None,
        };
    }
    Some(value)
}

/// A decimal array index without leading zeros (clause 16.2): `01` is not an index.
fn parse_index(part: &str) -> Option<usize> {
    let leading_zero = part.len() > 1 && part.starts_with('0');
    if part.is_empty() || leading_zero || !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// Converts a value to display text (clause 16.4). Numbers are written as ECMAScript
/// `Number.prototype.toString` writes them: `3.0` is `3` and `1e21` is `1e+21`.
#[must_use]
pub fn to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            // Always a double: serde_json's arbitrary precision is not enabled. `float_roundtrip`
            // is, so the double is the nearest one, as in ECMAScript.
            ryu_js::Buffer::new()
                .format(n.as_f64().unwrap_or_default())
                .to_string()
        }
        Value::Bool(b) => b.to_string(),
        Value::Null | Value::Object(_) => String::new(),
        Value::Array(items) => items.iter().map(to_text).collect::<Vec<_>>().join("\n"),
    }
}

/// The text for one field path: empty when the path is not found.
#[must_use]
pub fn field_text(fields: &Fields, path: &str) -> String {
    lookup(fields, path).map(to_text).unwrap_or_default()
}

/// A text pattern such as `{reference} · {version}` (clause 16.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Text(String),
    Field(String),
}

impl Pattern {
    /// Parses a pattern. `{{` and `}}` are literal braces.
    ///
    /// # Errors
    ///
    /// Fails for an unclosed or empty `{}`, or a lone `}`.
    pub fn parse(pattern: &str) -> Result<Pattern> {
        let mut parts = Vec::new();
        let mut text = String::new();
        let mut chars = pattern.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' if chars.peek() == Some(&'{') => {
                    chars.next();
                    text.push('{');
                }
                '}' if chars.peek() == Some(&'}') => {
                    chars.next();
                    text.push('}');
                }
                '{' => {
                    let mut path = String::new();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some('{') | None => {
                                return Err(Error::InvalidPattern(format!("unclosed \"{{\" in {pattern:?}")));
                            }
                            Some(c) => path.push(c),
                        }
                    }
                    // Only spaces and tabs around a path are ignored (clause 16.3).
                    let path = path.trim_matches([' ', '\t']).to_string();
                    if path.is_empty() {
                        return Err(Error::InvalidPattern(format!("empty \"{{}}\" in {pattern:?}")));
                    }
                    if !text.is_empty() {
                        parts.push(Part::Text(std::mem::take(&mut text)));
                    }
                    parts.push(Part::Field(path));
                }
                '}' => return Err(Error::InvalidPattern(format!("unexpected \"}}\" in {pattern:?}"))),
                c => text.push(c),
            }
        }
        if !text.is_empty() {
            parts.push(Part::Text(text));
        }
        Ok(Pattern { parts })
    }

    /// Fills in the pattern from `fields`. A cleared channel (empty `fields`) gives empty text,
    /// fixed text included (clauses 9.5 and 16.5).
    #[must_use]
    pub fn render(&self, fields: &Fields) -> String {
        if fields.is_empty() {
            return String::new();
        }
        self.parts
            .iter()
            .map(|p| match p {
                Part::Text(t) => t.clone(),
                Part::Field(path) => field_text(fields, path),
            })
            .collect()
    }

    /// The field paths the pattern uses, in order, e.g. `["reference", "version"]`. Apps can warn
    /// about a path that a type does not have, which would always render empty.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.parts.iter().filter_map(|p| match p {
            Part::Field(path) => Some(path.as_str()),
            Part::Text(_) => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fields() -> Fields {
        json!({
            "reference": "John 3:16",
            "version": "KJV",
            "verses": [{"number": 16, "text": "For God so loved the world"}],
            "lines": ["one", "two", 3],
            "on": true,
            "nothing": null,
            "obj": {"a": 1}
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn paths() {
        let f = fields();
        assert_eq!(field_text(&f, "reference"), "John 3:16");
        assert_eq!(field_text(&f, "verses.0.text"), "For God so loved the world");
        assert_eq!(field_text(&f, "verses.0.number"), "16");
        assert_eq!(field_text(&f, "verses.1.text"), "");
        assert_eq!(field_text(&f, "verses.+0.text"), "");
        // No leading zeros (clause 16.2).
        assert_eq!(field_text(&f, "verses.00.text"), "");
        assert_eq!(field_text(&f, "verses.01.text"), "");
        assert_eq!(field_text(&f, "missing"), "");
        assert_eq!(field_text(&f, "lines"), "one\ntwo\n3");
        assert_eq!(field_text(&f, "on"), "true");
        assert_eq!(field_text(&f, "nothing"), "");
        assert_eq!(field_text(&f, "obj"), "");
        assert_eq!(field_text(&Fields::new(), "text"), "");
    }

    #[test]
    fn numbers_as_ecmascript_writes_them() {
        // The table in clause 16.4, and examples/valid/update-numbers.json in the spec.
        let cases = [
            ("3", "3"),
            ("3.0", "3"),
            ("2.5", "2.5"),
            ("-0", "0"),
            ("0.1", "0.1"),
            ("1e21", "1e+21"),
            ("1e-7", "1e-7"),
            ("123456789012345678901", "123456789012345680000"),
            ("9007199254740993", "9007199254740992"),
        ];
        for (json, text) in cases {
            let value: Value = serde_json::from_str(json).unwrap();
            assert_eq!(to_text(&value), text, "{json}");
        }
    }

    #[test]
    fn patterns() {
        let f = fields();
        let p = Pattern::parse("{reference} · {version}").unwrap();
        assert_eq!(p.fields().collect::<Vec<_>>(), ["reference", "version"]);
        assert_eq!(p.render(&f), "John 3:16 · KJV");
        assert_eq!(
            Pattern::parse("{{literal}} {version}").unwrap().render(&f),
            "{literal} KJV"
        );
        assert_eq!(
            Pattern::parse("{ \treference\t }").unwrap().render(&f),
            "John 3:16"
        );
        // Only spaces and tabs are trimmed: a line break stays part of the path.
        assert_eq!(Pattern::parse("{reference\n}").unwrap().render(&f), "");
        // A cleared channel hides the fixed text too (clause 16.5).
        assert_eq!(p.render(&Fields::new()), "");
        for bad in ["{reference", "{}", "a } b", "{a{b}}"] {
            assert!(Pattern::parse(bad).is_err(), "{bad}");
        }
    }
}
