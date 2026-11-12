//! Pairing proofs (spec clause 13).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use unicode_normalization::UnicodeNormalization;

type HmacSha256 = Hmac<Sha256>;

/// Creates a new random nonce for a `hello` message (32 hex characters).
#[must_use]
pub fn new_nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// The HMAC keyed with the pairing code in NFC (clause 13.3), so a code typed with combining
/// marks matches the same code typed precomposed.
fn mac(code: &str, nonce: &str) -> HmacSha256 {
    let code: String = code.nfc().collect();
    let mut mac = HmacSha256::new_from_slice(code.as_bytes()).expect("HMAC accepts any key length");
    mac.update(nonce.as_bytes());
    mac
}

/// Computes the pairing proof: Base64 of HMAC-SHA256(key = code, message = nonce).
#[must_use]
pub fn proof(code: &str, nonce: &str) -> String {
    STANDARD.encode(mac(code, nonce).finalize().into_bytes())
}

/// Checks a pairing proof in constant time. A proof that is not Base64 of 32 bytes is wrong
/// (clause 13.4).
#[must_use]
pub fn verify(code: &str, nonce: &str, proof: &str) -> bool {
    let Ok(expected) = STANDARD.decode(proof) else {
        return false;
    };
    mac(code, nonce).verify_slice(&expected).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_spec_example() {
        // examples/valid/subscribe-pairing.json in the spec repository.
        let p = proof("123456", "q7cJ9mV2xL0aR4tY");
        assert_eq!(p, "i9tw3CG1t6+jl88QuoukjWJSEvC7mN3wlsoNQILA0mI=");
        assert!(verify("123456", "q7cJ9mV2xL0aR4tY", &p));
        assert!(!verify("123457", "q7cJ9mV2xL0aR4tY", &p));
        assert!(!verify("123456", "q7cJ9mV2xL0aR4tY", "not base64!"));
        assert!(!verify("123456", "q7cJ9mV2xL0aR4tY", &p[..40]));
    }

    #[test]
    fn codes_are_compared_in_nfc() {
        // "é" precomposed, and as "e" with a combining acute accent.
        let nonce = "q7cJ9mV2xL0aR4tY";
        assert!(verify("caf\u{e9}", nonce, &proof("cafe\u{301}", nonce)));
    }
}
