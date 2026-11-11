//! NTX 0.1 messages (spec clauses 7–15).
//!
//! [`Message::parse`] checks everything the JSON Schemas in the spec repository check, so
//! implementations built on this crate accept and reject the same messages.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{Error, MAX_MESSAGE_BYTES, PROGRAM, Result};

/// The fields of an item: any JSON object.
pub type Fields = Map<String, Value>;

/// Largest allowed sequence number (2^53 − 1, clause 9.4).
pub const MAX_SEQ: u64 = (1 << 53) - 1;

/// Identity of a source, receiver or publisher (clause 8.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// Stable identifier, ideally a UUID.
    pub id: String,
    /// Human-readable name, e.g. `MACHINE (App)`.
    pub name: String,
}

/// The pairing challenge in `hello` (clause 13.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingChallenge {
    /// Random string, new for every connection.
    pub nonce: String,
}

/// The pairing answer in `subscribe` or `publish` (clause 13.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingProof {
    /// Base64 of HMAC-SHA256(pairing code, nonce). Its format is checked with the proof itself,
    /// so a malformed proof is `pairing_failed` and a missing one `pairing_required`
    /// (clause 13.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<String>,
}

/// First message from a source or relay (clause 8.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    /// Protocol version.
    pub ntx: String,
    /// Who the source is.
    pub source: Endpoint,
    /// The source's channels; always includes `program`.
    pub channels: Vec<String>,
    /// Changes whenever the source restarts (clause 9.4).
    pub session: String,
    /// Present when pairing is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairing: Option<PairingChallenge>,
}

/// A receiver's answer to `hello` (clause 8.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscribe {
    /// Protocol version.
    pub ntx: String,
    /// Who the receiver is.
    pub receiver: Endpoint,
    /// Channels to receive; `program` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    /// Pairing proof, when the source asked for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairing: Option<PairingProof>,
}

/// A publisher's answer to a relay's `hello` (clause 14.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Publish {
    /// Protocol version.
    pub ntx: String,
    /// The source the relay will serve.
    pub source: Endpoint,
    /// The published channels; `program` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    /// Pairing proof, when the relay asked for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairing: Option<PairingProof>,
}

/// The types a source sends (clause 15.5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Announce {
    /// One descriptor per type.
    pub types: Vec<TypeDescriptor>,
}

/// The complete new item of a channel (clause 9.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    /// The channel, e.g. `program`.
    pub channel: String,
    /// Sequence number; newer items have larger numbers (clause 9.4).
    pub seq: u64,
    /// The item's type, e.g. `ntx.text`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// The item's fields; empty means "clear" (clause 9.5).
    pub fields: Fields,
}

/// Whether a receiver shows a source (clause 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Tally {
    /// Shown on air.
    pub program: bool,
    /// Shown in preview.
    pub preview: bool,
}

/// Body of `ping` and `pong` (clause 11.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Echoed back in the `pong`.
    pub id: String,
}

/// An error report (clause 12).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawError")]
pub struct ErrorMessage {
    /// What went wrong.
    pub code: ErrorCode,
    /// Optional text for logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// An `error` as sent, before its code is known to be one of ours.
#[derive(Deserialize)]
struct RawError {
    code: String,
    message: Option<String>,
}

impl TryFrom<RawError> for ErrorMessage {
    type Error = Error;

    /// An unknown code is treated as `bad_message` (clause 12.4); the code is kept at the start
    /// of the message, for logs.
    fn try_from(raw: RawError) -> Result<Self> {
        check_len("code", &raw.code, 1, 64)?;
        if let Some(text) = &raw.message {
            check_len("message", text, 0, MAX_ERROR_CHARS)?;
        }
        if let Some(code) = ErrorCode::known(&raw.code) {
            return Ok(ErrorMessage {
                code,
                message: raw.message,
            });
        }
        let text = match raw.message {
            Some(text) => format!("{}: {text}", raw.code),
            None => raw.code,
        };
        Ok(ErrorMessage {
            code: ErrorCode::BadMessage,
            message: Some(text.chars().take(MAX_ERROR_CHARS).collect()),
        })
    }
}

/// Longest `message` in `error`, in characters (clause 18.4).
const MAX_ERROR_CHARS: usize = 1024;

/// A type descriptor (clause 15.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    /// The type name, e.g. `com.example.lyrics`.
    #[serde(rename = "ntxType")]
    pub ntx_type: String,
    /// Short name for people.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Longer explanation for people.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema of the fields, limited to the allowed keywords.
    pub schema: Value,
}

/// Any NTX message (clause 7.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Message {
    /// See [`Hello`].
    Hello(Hello),
    /// See [`Subscribe`].
    Subscribe(Subscribe),
    /// See [`Publish`].
    Publish(Publish),
    /// See [`Announce`].
    Announce(Announce),
    /// See [`Update`].
    Update(Update),
    /// See [`Tally`].
    Tally(Tally),
    /// Heartbeat request.
    Ping(Heartbeat),
    /// Heartbeat answer.
    Pong(Heartbeat),
    /// See [`ErrorMessage`].
    Error(ErrorMessage),
}

const KNOWN_OPS: &[&str] = &[
    "hello",
    "subscribe",
    "publish",
    "announce",
    "update",
    "tally",
    "ping",
    "pong",
    "error",
];

impl Message {
    /// Parses and validates one message.
    ///
    /// Returns `Ok(None)` for a message with an unknown `op`, which must be ignored (clause 7.2).
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] over the size limit, [`Error::InvalidMessage`] for anything invalid.
    pub fn parse(text: &str) -> Result<Option<Message>> {
        if text.len() > MAX_MESSAGE_BYTES {
            return Err(Error::TooLarge(text.len()));
        }
        let value: Value = serde_json::from_str(text).map_err(|e| Error::InvalidMessage(e.to_string()))?;
        let op = value
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::InvalidMessage("missing \"op\"".into()))?;
        if !KNOWN_OPS.contains(&op) {
            return Ok(None);
        }
        let message: Message =
            serde_json::from_value(value).map_err(|e| Error::InvalidMessage(e.to_string()))?;
        message.validate()?;
        Ok(Some(message))
    }

    /// Serialises the message to JSON.
    #[must_use]
    ///
    /// # Panics
    ///
    /// Never: every NTX message can be represented as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("NTX messages always serialise")
    }

    /// Checks the rules that serde alone does not check.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidMessage`] or [`Error::InvalidTypeName`] describing the first problem.
    pub fn validate(&self) -> Result<()> {
        match self {
            Message::Hello(m) => {
                check_version(&m.ntx)?;
                check_endpoint(&m.source)?;
                check_channels(&m.channels, true)?;
                check_len("session", &m.session, 1, 128)?;
                if let Some(p) = &m.pairing {
                    check_len("pairing.nonce", &p.nonce, 16, 256)?;
                }
            }
            Message::Subscribe(m) => {
                check_version(&m.ntx)?;
                check_endpoint(&m.receiver)?;
                if let Some(c) = &m.channels {
                    check_channels(c, false)?;
                }
            }
            Message::Publish(m) => {
                check_version(&m.ntx)?;
                check_endpoint(&m.source)?;
                if let Some(c) = &m.channels {
                    check_channels(c, true)?;
                }
            }
            Message::Announce(m) => {
                for t in &m.types {
                    t.validate()?;
                }
            }
            Message::Update(m) => {
                check_len("channel", &m.channel, 1, 64)?;
                if m.seq > MAX_SEQ {
                    return Err(Error::InvalidMessage("seq is larger than 2^53 - 1".into()));
                }
                check_type_name(&m.type_name)?;
            }
            Message::Tally(_) => {}
            Message::Ping(h) | Message::Pong(h) => check_len("id", &h.id, 0, 64)?,
            Message::Error(e) => {
                if let Some(text) = &e.message {
                    check_len("message", text, 0, MAX_ERROR_CHARS)?;
                }
            }
        }
        Ok(())
    }
}

impl TypeDescriptor {
    /// Checks the descriptor, including the allowed JSON Schema keyword subset (clause 15.4).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTypeName`] or [`Error::InvalidMessage`] describing the problem.
    pub fn validate(&self) -> Result<()> {
        check_type_name(&self.ntx_type)?;
        for (what, text, max) in [
            ("title", &self.title, 256),
            ("description", &self.description, 4096),
        ] {
            if let Some(text) = text {
                check_len(what, text, 0, max)?;
            }
        }
        if self.schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err(Error::InvalidMessage(
                "a type descriptor schema must have \"type\": \"object\"".into(),
            ));
        }
        check_field_schema(&self.schema, "schema")
    }
}

/// Error codes (clause 12.2). An unknown code is received as [`ErrorCode::BadMessage`]
/// (clause 12.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// `unsupported_version`: the versions are not compatible.
    UnsupportedVersion,
    /// `pairing_required`: a pairing proof is missing.
    PairingRequired,
    /// `pairing_failed`: the pairing proof is wrong.
    PairingFailed,
    /// `too_large`: a message exceeds the size limit.
    TooLarge,
    /// `bad_message`: invalid JSON, or a required member is missing or wrong.
    BadMessage,
    /// `not_allowed`: a relay refused a publisher.
    NotAllowed,
}

impl ErrorCode {
    /// The code as sent on the wire.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::UnsupportedVersion => "unsupported_version",
            ErrorCode::PairingRequired => "pairing_required",
            ErrorCode::PairingFailed => "pairing_failed",
            ErrorCode::TooLarge => "too_large",
            ErrorCode::BadMessage => "bad_message",
            ErrorCode::NotAllowed => "not_allowed",
        }
    }

    /// The WebSocket close code that goes with this error (clause 12.2).
    #[must_use]
    pub fn close_code(&self) -> u16 {
        match self {
            ErrorCode::UnsupportedVersion => 4001,
            ErrorCode::PairingRequired | ErrorCode::PairingFailed | ErrorCode::NotAllowed => 4003,
            ErrorCode::TooLarge => 4009,
            ErrorCode::BadMessage => 4000,
        }
    }

    /// Whether reconnecting cannot help until the user changes something (clause 11.3).
    #[must_use]
    pub fn needs_user(&self) -> bool {
        !matches!(self, ErrorCode::TooLarge | ErrorCode::BadMessage)
    }

    fn known(code: &str) -> Option<ErrorCode> {
        [
            ErrorCode::UnsupportedVersion,
            ErrorCode::PairingRequired,
            ErrorCode::PairingFailed,
            ErrorCode::TooLarge,
            ErrorCode::BadMessage,
            ErrorCode::NotAllowed,
        ]
        .into_iter()
        .find(|c| c.as_str() == code)
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Checks a type name against `^[a-z0-9]+(\.[a-z0-9-]+)+$` (clause 15.2).
///
/// # Errors
///
/// [`Error::InvalidTypeName`] if it does not match.
pub fn check_type_name(name: &str) -> Result<()> {
    let mut parts = name.split('.');
    let first = parts.next().unwrap_or_default();
    let rest: Vec<&str> = parts.collect();
    let ok = name.len() <= 256
        && !first.is_empty()
        && first
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && !rest.is_empty()
        && rest.iter().all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        });
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidTypeName(name.to_string()))
    }
}

fn check_version(v: &str) -> Result<()> {
    let ok = v
        .split_once('.')
        .is_some_and(|(a, b)| is_digits(a) && is_digits(b));
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidMessage(format!("invalid version {v:?}")))
    }
}

fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn check_len(what: &str, s: &str, min: usize, max: usize) -> Result<()> {
    let n = s.chars().count();
    if n < min || n > max {
        return Err(Error::InvalidMessage(format!(
            "{what} must be {min} to {max} characters"
        )));
    }
    Ok(())
}

fn check_endpoint(e: &Endpoint) -> Result<()> {
    check_len("id", &e.id, 1, 128)?;
    check_len("name", &e.name, 1, 256)
}

fn check_channels(channels: &[String], needs_program: bool) -> Result<()> {
    for (i, c) in channels.iter().enumerate() {
        check_len("channel", c, 1, 64)?;
        if channels[..i].contains(c) {
            return Err(Error::InvalidMessage(format!("channel {c:?} is listed twice")));
        }
    }
    if needs_program && !channels.iter().any(|c| c == PROGRAM) {
        return Err(Error::InvalidMessage("channels must include \"program\"".into()));
    }
    Ok(())
}

const FIELD_TYPES: &[&str] = &[
    "object", "array", "string", "number", "integer", "boolean", "null",
];

/// Checks a field schema against `fieldSchema` in `type-descriptor.schema.json` (clause 15.4).
fn check_field_schema(schema: &Value, at: &str) -> Result<()> {
    let obj = schema
        .as_object()
        .ok_or_else(|| Error::InvalidMessage(format!("{at} must be an object")))?;
    for (key, value) in obj {
        let ok = match key.as_str() {
            "type" => value.as_str().is_some_and(|t| FIELD_TYPES.contains(&t)),
            "title" | "description" => value.is_string(),
            "properties" => {
                let props = value
                    .as_object()
                    .ok_or_else(|| Error::InvalidMessage(format!("{at}.properties must be an object")))?;
                for (name, sub) in props {
                    check_field_schema(sub, &format!("{at}.properties.{name}"))?;
                }
                true
            }
            "required" => value.as_array().is_some_and(|names| {
                names.iter().all(Value::is_string)
                    && names.iter().enumerate().all(|(i, n)| !names[..i].contains(n))
            }),
            "items" => {
                check_field_schema(value, &format!("{at}.items"))?;
                true
            }
            "enum" | "examples" => value.is_array(),
            "default" => true,
            "maxLength" => value.is_u64(),
            _ => {
                return Err(Error::InvalidMessage(format!(
                    "{at}: keyword {key:?} is not allowed in type descriptors"
                )));
            }
        };
        if !ok {
            return Err(Error::InvalidMessage(format!("{at}.{key} is not valid")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names() {
        for ok in ["ntx.text", "com.example.lyrics", "org.my-church.a1"] {
            assert!(check_type_name(ok).is_ok(), "{ok}");
        }
        for bad in ["Text", "text", "com.Example", ".a", "a.", "a..b", "com.ex_ample"] {
            assert!(check_type_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn unknown_op_is_ignored() {
        assert_eq!(Message::parse(r#"{"op":"com.example.future"}"#).unwrap(), None);
    }

    #[test]
    fn round_trip() {
        let m = Message::Update(Update {
            channel: "program".into(),
            seq: 1,
            type_name: "ntx.text".into(),
            fields: serde_json::json!({"text": "hi"}).as_object().unwrap().clone(),
        });
        assert_eq!(
            m.to_json(),
            r#"{"op":"update","channel":"program","seq":1,"type":"ntx.text","fields":{"text":"hi"}}"#
        );
        assert_eq!(Message::parse(&m.to_json()).unwrap(), Some(m));
    }

    #[test]
    fn unknown_error_codes_are_bad_message() {
        let parsed = Message::parse(r#"{"op":"error","code":"com.example.overheated","message":"hot"}"#);
        assert_eq!(
            parsed.unwrap(),
            Some(Message::Error(ErrorMessage {
                code: ErrorCode::BadMessage,
                message: Some("com.example.overheated: hot".into()),
            }))
        );
        let parsed = Message::parse(r#"{"op":"error","code":"later"}"#).unwrap();
        let Some(Message::Error(e)) = parsed else { panic!() };
        assert_eq!(e.message.as_deref(), Some("later"));
        // The message still fits its limit with the code in front.
        let long = format!(r#"{{"op":"error","code":"x","message":"{}"}}"#, "m".repeat(1024));
        let Some(Message::Error(e)) = Message::parse(&long).unwrap() else {
            panic!()
        };
        assert_eq!(e.message.unwrap().chars().count(), 1024);
        for bad in [
            r#"{"op":"error","code":""}"#,
            &format!(r#"{{"op":"error","code":"{}"}}"#, "c".repeat(65)),
            &format!(r#"{{"op":"error","code":"x","message":"{}"}}"#, "m".repeat(1025)),
        ] {
            assert!(Message::parse(bad).is_err(), "{bad}");
        }
        assert!(ErrorCode::NotAllowed.needs_user() && !ErrorCode::BadMessage.needs_user());
    }

    #[test]
    fn pairing_proofs_are_not_checked_by_the_parser() {
        // A malformed proof is pairing_failed and a missing one pairing_required (clause 13.4).
        for pairing in [r#"{"proof":"not-base64"}"#, "{}"] {
            let text = format!(
                r#"{{"op":"subscribe","ntx":"0.1","receiver":{{"id":"r","name":"R"}},"pairing":{pairing}}}"#
            );
            assert!(Message::parse(&text).unwrap().is_some(), "{text}");
        }
    }

    fn descriptor(schema: Value) -> TypeDescriptor {
        TypeDescriptor {
            ntx_type: "com.example.t".into(),
            title: None,
            description: None,
            schema,
        }
    }

    #[test]
    fn type_descriptor_limits() {
        let ok = serde_json::json!({
            "type": "object",
            "title": "T",
            "required": ["a"],
            "properties": {"a": {"type": "array", "items": {"type": "string", "maxLength": 5}, "enum": [], "examples": [], "default": 1}}
        });
        assert!(descriptor(ok).validate().is_ok());
        for bad in [
            serde_json::json!({"type": "object", "properties": {"a": {"type": "text"}}}),
            serde_json::json!({"type": "object", "title": 1}),
            serde_json::json!({"type": "object", "required": ["a", "a"]}),
            serde_json::json!({"type": "object", "required": [1]}),
            serde_json::json!({"type": "object", "enum": {}}),
            serde_json::json!({"type": "object", "maxLength": -1}),
            serde_json::json!({"type": "object", "properties": []}),
            serde_json::json!({"type": "object", "items": 1}),
            serde_json::json!({"type": "object", "pattern": "x"}),
            serde_json::json!({"type": "string"}),
        ] {
            assert!(descriptor(bad.clone()).validate().is_err(), "{bad}");
        }
        let mut long = descriptor(serde_json::json!({"type": "object"}));
        long.title = Some("t".repeat(257));
        assert!(long.validate().is_err());
        long.title = None;
        long.description = Some("d".repeat(4097));
        assert!(long.validate().is_err());
    }
}
