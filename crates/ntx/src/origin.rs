//! Web page origins: who may publish to a relay (clause 14.5) and who may read a source (8.7).

use std::collections::HashSet;
use std::net::IpAddr;

use crate::protocol::ErrorCode;
use crate::wire::{Ended, Peer};
use crate::{Error, Result};

/// Normalises a web origin, `http(s)://host[:port]`, for comparison: lowercase, without a
/// trailing slash or the scheme's default port. Other schemes cannot be allowed.
pub(crate) fn normalize_origin(origin: &str) -> Result<String> {
    let invalid = || Error::InvalidOrigin(origin.to_string());
    let (scheme, rest) = origin.trim().split_once("://").ok_or_else(invalid)?;
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    let scheme_ok = scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https");
    if !scheme_ok || rest.contains(['/', '?', '#', '@']) || rest.contains(char::is_whitespace) {
        return Err(invalid());
    }
    let (scheme, rest) = (scheme.to_ascii_lowercase(), rest.to_ascii_lowercase());
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !port.contains(']') => {
            (host, Some(port.parse::<u16>().map_err(|_| invalid())?))
        }
        _ => (rest.as_str(), None),
    };
    if host.is_empty() {
        return Err(invalid());
    }
    let default_port = matches!((scheme.as_str(), port), ("http", Some(80)) | ("https", Some(443)));
    Ok(match port {
        Some(port) if !default_port => format!("{scheme}://{host}:{port}"),
        _ => format!("{scheme}://{host}"),
    })
}

/// Whether a web page from `origin` may read a source (clause 8.7). Programs (no `Origin`), local
/// files (`null`) and pages on this computer or the local network may; other pages only when the
/// user allowed their origin (normalised in `allowed`).
pub(crate) fn may_read(origin: Option<&str>, allowed: &HashSet<String>) -> std::result::Result<(), String> {
    let Some(origin) = origin.filter(|o| *o != "null") else {
        return Ok(());
    };
    let origin = normalize_origin(origin).map_err(|e| e.to_string())?;
    if is_local(&origin) || allowed.contains(&origin) {
        Ok(())
    } else {
        Err(format!("pages from {origin} may not read this source"))
    }
}

/// Whether a normalised origin's host is on this computer or the local network: a loopback,
/// private or link-local address, `localhost`, or a `.local` name.
fn is_local(origin: &str) -> bool {
    let host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
    let host = match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(v6),
        None => host.rsplit_once(':').map_or(host, |(host, _)| host),
    };
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        Ok(IpAddr::V6(ip)) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
        // The origin is lowercase already.
        Err(_) => matches!(host.rsplit('.').next(), Some("localhost" | "local")),
    }
}

/// Refuses a connection with `not_allowed` instead of `hello` (clauses 8.7, 14.5).
pub(crate) async fn refuse<S>(peer: &mut Peer<S>, reason: String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let Ended(reason) = peer.reject(ErrorCode::NotAllowed, reason).await;
    tracing::debug!("refused a web page: {reason}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_normalised() {
        for (given, normal) in [
            ("https://Example.COM/", "https://example.com"),
            ("http://example.com:80", "http://example.com"),
            ("https://example.com:443", "https://example.com"),
            ("https://example.com:8443", "https://example.com:8443"),
            ("http://[::1]:3000", "http://[::1]:3000"),
            ("http://[::1]", "http://[::1]"),
        ] {
            assert_eq!(normalize_origin(given).unwrap(), normal, "{given}");
        }
        for bad in [
            "null",
            "example.com",
            "https://",
            "https://a/b",
            "https://a:x",
            "1http://a",
            "https://a b",
            "chrome-extension://abcdef",
            "file://x",
        ] {
            assert!(
                matches!(normalize_origin(bad), Err(Error::InvalidOrigin(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn local_pages_may_read_and_others_need_permission() {
        let allowed: HashSet<String> = ["https://lyrics.example".to_string()].into();
        for origin in [
            None,
            Some("null"),
            Some("http://localhost:8000"),
            Some("http://app.localhost"),
            Some("http://127.0.0.1:3000"),
            Some("http://192.168.1.20"),
            Some("http://10.0.0.5:8080"),
            Some("http://172.16.4.4"),
            Some("http://169.254.1.1"),
            Some("http://[::1]:8000"),
            Some("http://[fd00::5]"),
            Some("http://[fe80::1]"),
            Some("http://studio-pc.local"),
            Some("https://Lyrics.example/"),
        ] {
            assert_eq!(may_read(origin, &allowed), Ok(()), "{origin:?}");
        }
        for origin in [
            "https://evil.example",
            "http://8.8.8.8",
            "http://[2001:db8::1]",
            "file://x",
        ] {
            assert!(may_read(Some(origin), &allowed).is_err(), "{origin}");
        }
    }
}
