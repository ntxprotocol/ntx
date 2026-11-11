use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent};

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
                let mut known = self.known.lock().unwrap();
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
                // Prefer addresses other computers can reach: IPv4 first, loopback last.
                addresses.sort_by_key(|a| (a.is_loopback(), a.is_ipv6(), is_link_local_v6(a), *a));
                addresses.dedup();
                // Keep the address already reported while it is still valid, so the URL stays stable.
                let kept = previous
                    .and_then(|p| p.addresses.first().copied())
                    .filter(|a| addresses.contains(a) && !a.is_loopback());
                if let Some(k) = kept {
                    // The chosen address goes first, so `addresses[0]` is always the one in `url`.
                    addresses.retain(|a| *a != k);
                    addresses.insert(0, k);
                }
                let host = match addresses.first() {
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
                let (id, name) = self.known.lock().unwrap().remove(&fullname).map_or_else(
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
