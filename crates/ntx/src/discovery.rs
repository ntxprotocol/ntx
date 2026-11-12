use std::collections::HashMap;
use std::net::{IpAddr, UdpSocket};
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
    /// Several events can carry the same `id`: treat each one after the first as an update.
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
                let addresses = order_addresses(
                    addresses,
                    previous.and_then(|p| p.addresses.first()),
                    &default_networks(),
                );
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
                let mut known = lock(&self.known);
                let (id, name) = known.remove(&fullname).map_or_else(
                    || (fullname.clone(), instance_name(&fullname)),
                    |s| (s.id, s.name),
                );
                // Another record (say, the source under its old name) still has this id: the
                // source is not gone, it is now known by that record.
                Some(
                    known
                        .values()
                        .find(|s| s.id == id)
                        .map_or(BrowserEvent::Lost { id, name }, |s| {
                            BrowserEvent::Found(s.clone())
                        }),
                )
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

/// Orders addresses best first: those other computers can reach, then link-local, then loopback.
/// Among reachable ones, those on a network of the default route come first (not, say, a virtual
/// machine bridge), then IPv4 before IPv6. The address reported before stays first while it is
/// still known and as good as the best, so the URL does not change for nothing.
fn order_addresses(
    mut addresses: Vec<IpAddr>,
    reported: Option<&IpAddr>,
    networks: &[(IpAddr, u8)],
) -> Vec<IpAddr> {
    let rank = |a: &IpAddr| {
        (
            a.is_loopback(),
            is_link_local_v6(a),
            !networks.iter().any(|n| on_network(a, n)),
            a.is_ipv6(),
        )
    };
    addresses.sort_by_key(|a| (rank(a), *a));
    addresses.dedup();
    if let Some(&kept) = reported.filter(|r| addresses.first().is_some_and(|best| rank(r) == rank(best))) {
        if let Some(i) = addresses.iter().position(|a| *a == kept) {
            addresses[..=i].rotate_right(1);
        }
    }
    addresses
}

/// The networks (address, prefix length) of the interfaces that the default routes use. The
/// system picks the local address for an outside one; connecting a UDP socket sends nothing.
fn default_networks() -> Vec<(IpAddr, u8)> {
    let Ok(interfaces) = if_addrs::get_if_addrs() else {
        return Vec::new();
    };
    let local = |bind: &str, outside: &str| {
        let socket = UdpSocket::bind(bind).ok()?;
        socket.connect(outside).ok()?;
        socket.local_addr().ok().map(|a| a.ip())
    };
    // Documentation addresses (RFC 5737, RFC 3849): routed like any outside address.
    [
        local("0.0.0.0:0", "192.0.2.1:9"),
        local("[::]:0", "[2001:db8::1]:9"),
    ]
    .into_iter()
    .flatten()
    .filter_map(|ip| {
        interfaces.iter().find_map(|i| match &i.addr {
            if_addrs::IfAddr::V4(a) if IpAddr::V4(a.ip) == ip => Some((ip, a.prefixlen)),
            if_addrs::IfAddr::V6(a) if IpAddr::V6(a.ip) == ip => Some((ip, a.prefixlen)),
            _ => None,
        })
    })
    .collect()
}

/// Whether `address` is on the network `(address, prefix length)`.
fn on_network(address: &IpAddr, &(network, prefix): &(IpAddr, u8)) -> bool {
    let bits = |a: IpAddr| match a {
        IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => u128::from(v6),
    };
    let prefix = u32::from(prefix) + if network.is_ipv4() { 96 } else { 0 };
    address.is_ipv4() == network.is_ipv4()
        && (bits(*address) ^ bits(network))
            .checked_shr(128 - prefix)
            .unwrap_or(0)
            == 0
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
        assert_eq!(order_addresses(found, None, &[]), expected);
    }

    #[test]
    fn the_default_route_network_comes_first() {
        // A virtual machine bridge (192.168.64.1) and the LAN (192.168.1.0/24).
        let networks = [
            ("192.168.1.20".parse().unwrap(), 24),
            ("2001:db8:1::9".parse().unwrap(), 64),
        ];
        let found = ips(&["192.168.64.1", "2001:db8:2::5", "2001:db8:1::5", "192.168.1.7"]);
        let expected = ips(&["192.168.1.7", "2001:db8:1::5", "192.168.64.1", "2001:db8:2::5"]);
        assert_eq!(order_addresses(found, None, &networks), expected);
        // A bridge address reported before gives way to the LAN address.
        let found = ips(&["192.168.64.1", "192.168.1.7"]);
        assert_eq!(
            order_addresses(found, Some(&ips(&["192.168.64.1"])[0]), &networks)[0],
            ips(&["192.168.1.7"])[0]
        );
    }

    #[test]
    fn networks() {
        let lan = ("10.1.2.3".parse().unwrap(), 16);
        assert!(on_network(&ips(&["10.1.200.9"])[0], &lan));
        assert!(!on_network(&ips(&["10.2.0.1"])[0], &lan));
        assert!(!on_network(&ips(&["::ffff:10.1.0.1"])[0], &lan));
        assert!(on_network(
            &ips(&["8.8.8.8"])[0],
            &("10.0.0.1".parse().unwrap(), 0)
        ));
        assert!(on_network(&ips(&["::1"])[0], &("::1".parse().unwrap(), 128)));
        assert!(!on_network(&ips(&["::2"])[0], &("::1".parse().unwrap(), 128)));
        // Every address found is a local one, on a network that holds it.
        for network in default_networks() {
            assert!(on_network(&network.0, &network));
        }
    }

    #[test]
    fn the_reported_address_stays_only_while_it_is_as_good() {
        let reported: IpAddr = "192.0.2.9".parse().unwrap();
        let found = ips(&["192.0.2.7", "192.0.2.9", "fe80::1"]);
        assert_eq!(order_addresses(found, Some(&reported), &[])[0], reported);
        // Reported first, a link-local address gives way once an IPv4 address is known.
        let link_local: IpAddr = "fe80::1".parse().unwrap();
        let found = ips(&["fe80::1", "192.0.2.7"]);
        assert_eq!(
            order_addresses(found, Some(&link_local), &[])[0],
            ips(&["192.0.2.7"])[0]
        );
    }
}
