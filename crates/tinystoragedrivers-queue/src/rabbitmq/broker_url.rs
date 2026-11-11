//! The plaintext-credential rule for broker URLs.

use std::net::{Ipv4Addr, Ipv6Addr};

use tinystoragedrivers_types::{Result, StorageError};

/// The query parameter that opts a plaintext URL with credentials in.
const INSECURE: &str = "insecure";

/// Check a broker URL against the plaintext-credential rule and return the URL
/// to connect with.
///
/// `amqp://` sends the credentials in clear text, so a URL that carries
/// userinfo must use `amqps://` unless its host is loopback (`localhost`,
/// `127.0.0.0/8` or `::1`) or it opts in with `?insecure=true` for a trusted
/// private network. Every `insecure` parameter is removed from the returned URL,
/// so the opt-in never reaches the broker.
///
/// # Errors
/// `InvalidInput` for a non-loopback `amqp://` URL with credentials and no
/// opt-in. The message never contains the URL or its credentials.
pub fn check_broker_url(url: &str) -> Result<String> {
    let (stripped, opted_in) = strip_insecure(url);
    let Some((scheme, rest)) = stripped.split_once("://") else {
        return Ok(stripped);
    };
    if !scheme.eq_ignore_ascii_case("amqp") || opted_in {
        return Ok(stripped);
    }
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    let Some((userinfo, host)) = authority.rsplit_once('@') else {
        return Ok(stripped);
    };
    if userinfo.is_empty() || is_loopback(host) {
        return Ok(stripped);
    }
    Err(StorageError::invalid_input(
        "a broker URL with credentials must use amqps:// unless its host is loopback \
         or it opts in with ?insecure=true",
    ))
}

/// Remove every `insecure` query parameter, reporting whether one was `true`.
fn strip_insecure(url: &str) -> (String, bool) {
    let Some((base, after)) = url.split_once('?') else {
        return (url.to_owned(), false);
    };
    let (query, fragment) = match after.split_once('#') {
        Some((query, fragment)) => (query, Some(fragment)),
        None => (after, None),
    };
    let mut opted_in = false;
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if key == INSECURE {
                opted_in |= value == "true";
                return false;
            }
            !pair.is_empty()
        })
        .collect();
    let mut out = base.to_owned();
    if !kept.is_empty() {
        out.push('?');
        out.push_str(&kept.join("&"));
    }
    if let Some(fragment) = fragment {
        out.push('#');
        out.push_str(fragment);
    }
    (out, opted_in)
}

/// Whether `host` (an authority without userinfo, port included) is loopback.
fn is_loopback(host: &str) -> bool {
    if let Some(bracketed) = host.strip_prefix('[') {
        return bracketed
            .split_once(']')
            .and_then(|(address, _)| address.parse::<Ipv6Addr>().ok())
            .is_some_and(|address| address.is_loopback());
    }
    let name = host.rsplit_once(':').map_or(host, |(name, _)| name);
    name.eq_ignore_ascii_case("localhost")
        || name
            .parse::<Ipv4Addr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
#[path = "broker_url_tests.rs"]
mod tests;
