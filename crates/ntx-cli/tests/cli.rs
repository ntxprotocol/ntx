//! Surface tests: run the real `ntx` binary the way a user does.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const NTX: &str = env!("CARGO_BIN_EXE_ntx");
const WAIT: Duration = Duration::from_secs(10);

/// A running `ntx` command, with its standard input and a feed of its log lines.
struct Running {
    child: Child,
    stdin: ChildStdin,
    log: mpsc::Receiver<String>,
}

impl Running {
    fn start(args: &[&str]) -> Running {
        let mut child = Command::new(NTX)
            .args(args)
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let (tx, log) = mpsc::channel();
        let stderr = BufReader::new(child.stderr.take().unwrap());
        std::thread::spawn(move || {
            stderr
                .lines()
                .map_while(Result::ok)
                .for_each(|l| drop(tx.send(l)));
        });
        Running { child, stdin, log }
    }

    fn line(&mut self, text: &str) {
        writeln!(self.stdin, "{text}").unwrap();
    }

    /// Waits for a log line containing `needle`.
    fn expect(&self, needle: &str) -> String {
        loop {
            let line = self
                .log
                .recv_timeout(WAIT)
                .unwrap_or_else(|_| panic!("no log line with {needle:?}"));
            if line.contains(needle) {
                return line;
            }
        }
    }

    /// Waits for a log line containing `needle` followed by a port or a URL with a port, and
    /// returns `127.0.0.1:<port>`.
    fn expect_port(&self, needle: &str) -> String {
        let line = self.expect(needle);
        let after = line.split(needle).nth(1).unwrap();
        let address = after.split(['/', ' ', ',']).next().unwrap();
        format!("127.0.0.1:{}", address.rsplit(':').next().unwrap())
    }

    /// Closes standard input and returns the exit status.
    fn finish(mut self) -> std::process::ExitStatus {
        drop(self.stdin);
        self.child.wait().unwrap()
    }
}

/// Starts `ntx send` on any free port; returns it and its address.
fn sender(extra: &[&str]) -> (Running, String) {
    let name: &[&str] = if extra.contains(&"--name") {
        &[]
    } else {
        &["--name", "surface test"]
    };
    let sender = Running::start(&[&["send", "--port", "0"], name, extra].concat());
    let address = sender.expect_port("listening on ws://");
    (sender, address)
}

/// Runs `ntx recv` until it has printed `count` updates.
fn recv(address: &str, count: usize, extra: &[&str]) -> std::thread::JoinHandle<Output> {
    let mut command = Command::new(NTX);
    command
        .args(["recv", address, "--count", &count.to_string()])
        .args(extra);
    std::thread::spawn(move || command.output().unwrap())
}

fn stdout(output: &Output) -> Vec<String> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(String::from)
        .collect()
}

#[test]
fn help_and_version() {
    let help = Command::new(NTX).arg("--help").output().unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("send"));
    let version = Command::new(NTX).arg("--version").output().unwrap();
    assert!(String::from_utf8_lossy(&version.stdout).starts_with("ntx "));
    assert!(
        !Command::new(NTX)
            .arg("nonsense")
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn send_and_receive_text() {
    let (mut sender, address) = sender(&["--no-mdns"]);
    sender.line("Welcome");
    sender.expect("sent seq 1");
    let receiver = recv(&address, 3, &["--field", "text", "--on-air"]);
    sender.expect("+ receiver");
    sender.expect("tally: on air true");
    sender.line("Amazing grace");
    sender.expect("sent seq 2");
    sender.line(""); // clears
    let output = receiver.join().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(stdout(&output), ["Welcome", "Amazing grace", ""]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("connected to"));
    sender.expect("- receiver");
    assert!(sender.finish().success());
}

#[test]
fn json_fields_patterns_and_preview() {
    let (mut sender, address) = sender(&[
        "--no-mdns",
        "--json",
        "--type",
        "com.example.scripture",
        "--preview",
        "--channel",
        "preview",
    ]);
    let receiver = recv(
        &address,
        2,
        &["--channel", "preview", "--pattern", "{reference} · {version}"],
    );
    sender.expect("+ receiver");
    sender.line(r#"{"reference": "John 3:16", "version": "KJV"}"#);
    let output = receiver.join().unwrap();
    // The channel starts cleared, which hides the pattern's fixed text too (clause 16.5).
    assert_eq!(stdout(&output), ["", "John 3:16 · KJV"]);
    assert!(sender.finish().success());
}

#[test]
fn raw_json_output_and_pairing() {
    let (mut sender, address) = sender(&["--no-mdns", "--pair", "2468", "--id", "fixed-id"]);
    sender.line("secret");
    sender.expect("sent seq 1");
    let receiver = recv(&address, 1, &["--pair", "2468"]);
    let output = receiver.join().unwrap();
    let update: serde_json::Value = serde_json::from_str(&stdout(&output)[0]).unwrap();
    assert_eq!(update["fields"]["text"], "secret");
    assert_eq!(update["type"], "ntx.text");

    // Without the code, or with a wrong one, recv stops instead of retrying (clause 11.3).
    for extra in [&[][..], &["--pair", "1111"]] {
        let output = recv(&address, 1, extra).join().unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("source error: pairing_"), "{stderr}");
    }
    assert!(sender.finish().success());
}

#[test]
fn bad_input_fails_cleanly() {
    // A line that cannot be sent is reported and skipped: receivers keep their source.
    let (mut sender, _) = sender(&["--no-mdns", "--json"]);
    sender.line("not json");
    sender.expect("not sent: ");
    sender.line(&format!(r#"{{"text":"{}"}}"#, "x".repeat(70_000)));
    sender.expect("not sent: message is");
    sender.line(r#"{"text":"ok"}"#);
    sender.expect("sent seq 1");
    assert!(sender.finish().success());

    let bad_pattern = Command::new(NTX)
        .args(["recv", "127.0.0.1", "--pattern", "{oops"])
        .output()
        .unwrap();
    assert!(!bad_pattern.status.success());
    assert!(String::from_utf8_lossy(&bad_pattern.stderr).contains("invalid pattern"));

    let bad_address = Command::new(NTX).args(["recv", "http://x"]).output().unwrap();
    assert!(String::from_utf8_lossy(&bad_address.stderr).contains("invalid address"));
}

#[test]
fn list_finds_an_advertised_sender() {
    // Longer than a DNS label (63 bytes): the advertised name must be shortened, not dropped.
    let name = format!("surface test {}", "x".repeat(60));
    let (sender, _) = sender(&["--id", "surface-list", "--name", &name]);
    let output = Command::new(NTX)
        .args(["list", "--seconds", "6"])
        .output()
        .unwrap();
    assert!(output.status.success());
    // Lines look like "<name>  <url>  NTX <version>  id <id>". Long host names (as on CI) can
    // push "surface test" past the 63-byte cut, so check the id and the shortened name.
    let host = gethostname::gethostname().to_string_lossy().into_owned();
    let machine = host.split('.').next().unwrap_or(&host).to_uppercase();
    let full_name = format!("{machine} ({name})");
    let listed = stdout(&output)
        .into_iter()
        .find(|l| l.ends_with("id surface-list"));
    let shown = listed
        .as_deref()
        .and_then(|l| l.split("  ws://").next())
        .unwrap_or_default();
    assert!(
        shown.len() <= 63 && !shown.is_empty() && full_name.starts_with(shown),
        "{output:?}"
    );
    assert!(sender.finish().success());

    let nothing = Command::new(NTX)
        .args(["list", "--seconds", "0"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&nothing.stderr).contains("no NTX sources found"));
}

/// Starts `ntx relay` on any free port; returns it and its address.
fn relay(extra: &[&str]) -> (Running, String) {
    let relay = Running::start(&[&["relay", "--port", "0", "--no-mdns"], extra].concat());
    let address = relay.expect_port("listening on ws://");
    (relay, address)
}

#[test]
fn publish_through_a_relay() {
    let (relay, relay_address) = relay(&["--pair", "1357"]);
    let url = format!("ws://{relay_address}/publish");

    // Without the relay's code, publish stops instead of retrying (clause 11.3).
    let unpaired = Running::start(&["publish", &url]);
    unpaired.expect("relay error: pairing_required");
    assert!(!unpaired.finish().success());

    let mut publisher = Running::start(&[
        "publish",
        &relay_address,
        "--name",
        "surface publish",
        "--pair",
        "1357",
        "--preview",
    ]);
    publisher.expect("connected to relay");
    publisher.line("Welcome");
    publisher.expect("sent seq 1");
    let source = relay.expect_port("at ws://");
    let receiver = recv(&source, 2, &["--field", "text", "--on-air"]);
    publisher.expect("tally: on air true");
    publisher.line("Amazing grace");
    let output = receiver.join().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(stdout(&output), ["Welcome", "Amazing grace"]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("(surface publish)"));
    assert!(publisher.finish().success());
    relay.expect("- publisher");
    assert!(relay.finish().success());
}

#[test]
fn relay_refuses_web_pages_unless_allowed() {
    let (relay, address) = relay(&["--allow-origin", "https://ok.example"]);
    let connect = |origin: &str| {
        let mut request =
            tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{address}/publish"))
                .unwrap();
        request.headers_mut().insert("origin", origin.parse().unwrap());
        let (mut ws, _) = tungstenite::connect(request).unwrap();
        ws.read().unwrap().into_text().unwrap().to_string()
    };
    assert!(connect("https://bad.example").contains("not_allowed"));
    relay.expect("refused a page from https://bad.example");
    assert!(connect("https://ok.example").contains("\"hello\""));
    assert!(relay.finish().success());
}

/// Accepts one WebSocket connection on `listener` as a fake source or relay, and says `hello`.
#[allow(clippy::result_large_err)] // the callback's error type comes from tungstenite
fn accept_fake(listener: &std::net::TcpListener) -> tungstenite::WebSocket<std::net::TcpStream> {
    let (stream, _) = listener.accept().unwrap();
    let echo_ntx = |_: &tungstenite::handshake::server::Request,
                    mut response: tungstenite::handshake::server::Response| {
        let headers = response.headers_mut();
        headers.insert("sec-websocket-protocol", "ntx".parse().unwrap());
        Ok(response)
    };
    let mut ws = tungstenite::accept_hdr(stream, echo_ntx).unwrap();
    let hello = r#"{"op":"hello","ntx":"0.1","source":{"id":"fake","name":"FAKE"},"channels":["program"],"session":"s"}"#;
    ws.send(tungstenite::Message::text(hello)).unwrap();
    ws
}

#[test]
fn recv_closes_its_connection_before_it_exits() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let receiver = recv(&address, 1, &[]);
    let mut ws = accept_fake(&listener);
    let update = r#"{"op":"update","channel":"program","seq":1,"type":"ntx.text","fields":{"text":"hi"}}"#;
    ws.send(tungstenite::Message::text(update)).unwrap();
    assert!(receiver.join().unwrap().status.success());
    // The source gets a close frame, not a connection reset (RFC 6455 section 7).
    loop {
        match ws.read() {
            Ok(tungstenite::Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => break,
            Ok(_) => {}
            Err(e) => panic!("no close frame: {e}"),
        }
    }
}

#[test]
fn piped_input_reaches_the_relay_before_publish_exits() {
    // A fake relay that records what the publisher sends.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}/publish", listener.local_addr().unwrap());
    let relay = std::thread::spawn(move || {
        let mut ws = accept_fake(&listener);
        let mut seen = Vec::new();
        while let Ok(message) = ws.read() {
            if let tungstenite::Message::Text(text) = message {
                seen.push(text.to_string());
            }
        }
        seen
    });
    let mut command = Command::new(NTX)
        .args(["publish", &url])
        .stdin(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    writeln!(command.stdin.take().unwrap(), "hi").unwrap();
    assert!(command.wait().unwrap().success());
    let seen = relay.join().unwrap();
    assert!(seen.iter().any(|m| m.contains(r#""text":"hi""#)), "{seen:?}");
}

#[test]
fn relay_refuses_an_invalid_allowed_origin() {
    let output = Command::new(NTX)
        .args([
            "relay",
            "--port",
            "0",
            "--no-mdns",
            "--allow-origin",
            "example.com",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid origin"));
}

#[test]
fn send_closes_its_connections_before_it_exits() {
    let (mut sender, address) = sender(&["--no-mdns"]);
    let mut request =
        tungstenite::client::IntoClientRequest::into_client_request(format!("ws://{address}/")).unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "ntx".parse().unwrap());
    let (mut ws, _) = tungstenite::connect(request).unwrap();
    let subscribe = r#"{"op":"subscribe","ntx":"0.1","receiver":{"id":"surface","name":"surface test"}}"#;
    ws.send(tungstenite::Message::text(subscribe)).unwrap();
    sender.expect("+ receiver");
    sender.line("last line");
    sender.expect("sent seq 1");
    assert!(sender.finish().success());
    // The receiver gets a close frame, not a connection reset (RFC 6455 section 7).
    loop {
        match ws.read() {
            Ok(tungstenite::Message::Close(_)) | Err(tungstenite::Error::ConnectionClosed) => break,
            Ok(_) => {}
            Err(e) => panic!("no close frame: {e}"),
        }
    }
}
