//! Surface tests: run the real `ntx` binary the way a user does.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const NTX: &str = env!("CARGO_BIN_EXE_ntx");
const WAIT: Duration = Duration::from_secs(10);

/// A running `ntx send`, with its address and a feed of its log lines.
struct Sender {
    child: Child,
    stdin: ChildStdin,
    address: String,
    log: mpsc::Receiver<String>,
}

impl Sender {
    fn start(extra: &[&str]) -> Sender {
        let mut child = Command::new(NTX)
            .args(["send", "--port", "0"])
            .args(if extra.contains(&"--name") {
                &[][..]
            } else {
                &["--name", "surface test"][..]
            })
            .args(extra)
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
        let mut sender = Sender {
            child,
            stdin,
            address: String::new(),
            log,
        };
        let line = sender.expect("listening on ws://");
        let port = line
            .split("ws://")
            .nth(1)
            .unwrap()
            .split('/')
            .next()
            .unwrap()
            .rsplit(':')
            .next()
            .unwrap();
        sender.address = format!("127.0.0.1:{port}");
        sender
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

    /// Closes standard input and returns the exit status.
    fn finish(mut self) -> std::process::ExitStatus {
        drop(self.stdin);
        self.child.wait().unwrap()
    }
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
    let mut sender = Sender::start(&["--no-mdns"]);
    sender.line("Welcome");
    sender.expect("sent seq 1");
    let receiver = recv(&sender.address, 3, &["--field", "text", "--on-air"]);
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
    let mut sender = Sender::start(&[
        "--no-mdns",
        "--json",
        "--type",
        "com.example.scripture",
        "--preview",
        "--channel",
        "preview",
    ]);
    let receiver = recv(
        &sender.address,
        2,
        &["--channel", "preview", "--pattern", "{reference} · {version}"],
    );
    sender.expect("+ receiver");
    sender.line(r#"{"reference": "John 3:16", "version": "KJV"}"#);
    let output = receiver.join().unwrap();
    assert_eq!(stdout(&output), [" · ", "John 3:16 · KJV"]);
    assert!(sender.finish().success());
}

#[test]
fn raw_json_output_and_pairing() {
    let mut sender = Sender::start(&["--no-mdns", "--pair", "2468", "--id", "fixed-id"]);
    sender.line("secret");
    sender.expect("sent seq 1");
    let receiver = recv(&sender.address, 1, &["--pair", "2468"]);
    let output = receiver.join().unwrap();
    let update: serde_json::Value = serde_json::from_str(&stdout(&output)[0]).unwrap();
    assert_eq!(update["fields"]["text"], "secret");
    assert_eq!(update["type"], "ntx.text");
    assert!(sender.finish().success());
}

#[test]
fn bad_input_fails_cleanly() {
    let mut sender = Sender::start(&["--no-mdns", "--json"]);
    sender.line("not json");
    let status = sender.finish();
    assert!(!status.success());

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
    let sender = Sender::start(&["--id", "surface-list", "--name", &name]);
    let output = Command::new(NTX)
        .args(["list", "--seconds", "6"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        stdout(&output)
            .iter()
            .any(|l| l.contains("surface test") && l.contains("id surface-list")),
        "{output:?}"
    );
    assert!(sender.finish().success());

    let nothing = Command::new(NTX)
        .args(["list", "--seconds", "0"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&nothing.stderr).contains("no NTX sources found"));
}
