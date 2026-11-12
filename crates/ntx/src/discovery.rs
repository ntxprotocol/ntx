use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent};

use crate::wire::lock;
use crate::{Error, Result, SERVICE_TYPE};

/// A source found on the network (clause 6).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DiscoveredSource {
    /// The source id from the TXT record.
    pub id: String,
    /// The DNS-SD instance name, normally the source's display name.
    pub name: String,
    /// WebSocket URL to connect to.
    pub url: String,
    /// Protocol version from the TXT record.
    pub version: String,
    /// The source needs a pairing code.
    pub pairing_required: bool,
    /// The host name from mDNS.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// All known addresses; the first one is used in `url`.
    pub addresses: Vec<IpAddr>,
}

/// Something a [`Browser`] noticed.
///
/// Serialises as `{"event": "found", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum BrowserEvent {
    /// A new source, or a change of URL, name, version or pairing for a source already found.
    Found(DiscoveredSource),
    /// A source went away.
    Lost {
        /// The source id.
        id: String,
        /// The source name.
        name: String,
    },
}

/// Finds NTX sources with mDNS.
pub struct Browser {
    daemon: ServiceDaemon,
    events: mdns_sd::Receiver<ServiceEvent>,
    // fullname -> what was last reported, to merge addresses, skip repeats and report removals by id.
    known: Mutex<HashMap<String, DiscoveredSource>>,
}

impl Browser {
    /// Starts looking for sources.
    ///
    /// # Errors
    ///
    /// Fails if mDNS cannot start.
    pub fn start() -> Result<Browser> {
        let mdns = |e: mdns_sd::Error| Error::Mdns(e.to_string());
        let daemon = ServiceDaemon::new().map_err(mdns)?;
        let events = daemon.browse(SERVICE_TYPE).map_err(mdns)?;
        Ok(Browser {
            daemon,
            events,
            known: Mutex::new(HashMap::new()),
        })
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<BrowserEvent> {
        while let Ok(event) = self.events.try_recv() {
            if let Some(e) = self.convert(event) {
                return Some(e);
            }
        }
        None
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<BrowserEvent> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let event = self.events.recv_timeout(left).ok()?;
            if let Some(e) = self.convert(event) {
                return Some(e);
            }
        }
    }

    fn convert(&self, event: ServiceEvent) -> Option<BrowserEvent> {
        match event {
            ServiceEvent::ServiceResolved(info) => {
                let name = instance_name(&info.fullname);
                let id = info
                    .get_property_val_str("id")
                    .unwrap_or(&info.fullname)
                    .to_string();
                let version = info.get_property_val_str("ntx").unwrap_or_default().to_string();
                let pairing_required = info.get_property_val_str("pair") == Some("1");
                let tls = info.get_property_val_str("tls") == Some("1");
                let path = info.get_property_val_str("path").unwrap_or("/").to_string();
                let mut known = lock(&self.known);
                let previous = known.get(&info.fullname);
                // mDNS reports each network interface separately, so collect all addresses.
                let mut addresses: Vec<IpAddr> = info
                    .get_addresses()
                    .iter()
                    .map(mdns_sd::ScopedIp::to_ip_addr)
                    .collect();
                if let Some(p) = previous.filter(|p| p.port == info.port) {
                    addresses.extend(p.addresses.iter().copied());
                }
                let addresses = order_addresses(addresses, previous.and_then(|p| p.addresses.first()));
                // A link-local address cannot go in a URL without its interface, so then use the
                // host name.
                let host = match addresses.first().filter(|a| !is_link_local_v6(a)) {
                    Some(IpAddr::V4(a)) => a.to_string(),
                    Some(IpAddr::V6(a)) => format!("[{a}]"),
                    None => info.host.trim_end_matches('.').to_string(),
                };
                let scheme = if tls { "wss" } else { "ws" };
                let path = if path.starts_with('/') {
                    path
                } else {
                    format!("/{path}")
                };
                let url = format!("{scheme}://{host}:{}{path}", info.port);
                let found = DiscoveredSource {
                    id,
                    name,
                    url,
                    version,
                    pairing_required,
                    host: info.host.clone(),
                    port: info.port,
                    addresses,
                };
                // Only report what a user would notice; a longer address list alone is not news.
                let news = previous.is_none_or(|p| {
                    (&p.id, &p.name, &p.url, &p.version, p.pairing_required)
                        != (
                            &found.id,
                            &found.name,
                            &found.url,
                            &found.version,
                            found.pairing_required,
                        )
                });
                known.insert(info.fullname.clone(), found.clone());
                news.then_some(BrowserEvent::Found(found))
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                let (id, name) = lock(&self.known).remove(&fullname).map_or_else(
                    || (fullname.clone(), instance_name(&fullname)),
                    |s| (s.id, s.name),
                );
                Some(BrowserEvent::Lost { id, name })
            }
            _ => None,
        }
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.daemon.stop_browse(SERVICE_TYPE);
        let _ = self.daemon.shutdown();
    }
}

/// Orders addresses best first: those other computers can reach (IPv4, then IPv6), then
/// link-local, then loopback. The address reported before stays first while it is still known
/// and as good as the best, so the URL does not change for nothing.
fn order_addresses(mut addresses: Vec<IpAddr>, reported: Option<&IpAddr>) -> Vec<IpAddr> {
    let rank = |a: &IpAddr| (a.is_loopback(), is_link_local_v6(a), a.is_ipv6());
    addresses.sort_by_key(|a| (rank(a), *a));
    addresses.dedup();
    if let Some(&kept) = reported.filter(|r| addresses.first().is_some_and(|best| rank(r) == rank(best))) {
        if let Some(i) = addresses.iter().position(|a| *a == kept) {
            addresses[..=i].rotate_right(1);
        }
    }
    addresses
}

/// IPv6 link-local addresses (`fe80::/10`) need an interface scope to be usable in a URL.
fn is_link_local_v6(address: &IpAddr) -> bool {
    matches!(address, IpAddr::V6(v6) if v6.segments()[0] & 0xffc0 == 0xfe80)
}

fn instance_name(fullname: &str) -> String {
    fullname
        .strip_suffix(&format!(".{SERVICE_TYPE}"))
        .unwrap_or(fullname)
        .replace("\\.", ".")
        .replace("\\\\", "\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ips(list: &[&str]) -> Vec<IpAddr> {
        list.iter().map(|a| a.parse().unwrap()).collect()
    }

    #[test]
    fn reachable_addresses_come_first() {
        let found = ips(&["::1", "fe80::1", "2001:db8::5", "127.0.0.1", "192.0.2.7"]);
        let expected = ips(&["192.0.2.7", "2001:db8::5", "fe80::1", "127.0.0.1", "::1"]);
        assert_eq!(order_addresses(found, None), expected);
    }

    #[test]
    fn the_reported_address_stays_only_while_it_is_as_good() {
        let reported: IpAddr = "192.0.2.9".parse().unwrap();
        let found = ips(&["192.0.2.7", "192.0.2.9", "fe80::1"]);
        assert_eq!(order_addresses(found, Some(&reported))[0], reported);
        // Reported first, a link-local address gives way once an IPv4 address is known.
        let link_local: IpAddr = "fe80::1".parse().unwrap();
        let found = ips(&["fe80::1", "192.0.2.7"]);
        assert_eq!(
            order_addresses(found, Some(&link_local))[0],
            ips(&["192.0.2.7"])[0]
        );
    }
}
