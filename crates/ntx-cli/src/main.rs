//! `ntx`: a small test sender, receiver, publisher and relay for NTX.

use std::error::Error;
use std::io::BufRead;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use ntx::binding::{Pattern, field_text};
use ntx::{
    Browser, BrowserEvent, Fields, Publisher, PublisherConfig, PublisherEvent, Receiver, ReceiverConfig,
    ReceiverEvent, Relay, RelayConfig, RelayEvent, Source, SourceConfig, SourceEvent, Tally, display_name,
};

type CliResult = Result<(), Box<dyn Error>>;

/// Set by Ctrl-C. Commands then stop like at the end of their input, so what runs is dropped
/// normally and closes its connections properly instead of resetting them.
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::Relaxed)
}

#[derive(Parser)]
#[command(
    name = "ntx",
    version,
    about = "Test sender and receiver for NTX (Network Text Exchange)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start a source and send each line read from standard input.
    ///
    /// Plain text lines are sent as `ntx.text`. With --json, each line is a fields object.
    /// An empty line clears the channel. The source stops at the end of the input.
    Send(SendArgs),
    /// Connect to a source and print what it sends.
    Recv(RecvArgs),
    /// List sources found on the network with mDNS.
    List(ListArgs),
    /// Send each line read from standard input through a relay, like `send`.
    ///
    /// The relay serves the text to the network as a source. Use this where a source cannot
    /// accept connections. The publisher stops at the end of the input.
    Publish(PublishArgs),
    /// Run a relay: serve publishers to the network as sources.
    ///
    /// Web pages may publish only from origins allowed with --allow-origin; programs that send
    /// no Origin header only from this computer. The relay stops at the end of standard input.
    Relay(RelayArgs),
}

#[derive(Args)]
struct SendArgs {
    /// Source name (the machine name is added in front).
    #[arg(long, default_value = "ntx send")]
    name: String,
    /// TCP port (0 = any free port).
    #[arg(long, default_value_t = ntx::DEFAULT_PORT)]
    port: u16,
    /// Require this pairing code.
    #[arg(long)]
    pair: Option<String>,
    /// Do not advertise with mDNS.
    #[arg(long)]
    no_mdns: bool,
    #[command(flatten)]
    lines: LineArgs,
}

#[derive(Args)]
struct PublishArgs {
    /// Relay address: ws:// URL, `host` or `host:port` (with the path /publish).
    #[arg(default_value = ntx::DEFAULT_RELAY_URL)]
    relay: String,
    /// Name of the published source (the relay adds its machine name).
    #[arg(long, default_value = "ntx publish")]
    name: String,
    /// The relay's pairing code.
    #[arg(long)]
    pair: Option<String>,
    #[command(flatten)]
    lines: LineArgs,
}

/// What `send` and `publish` send.
#[derive(Args)]
struct LineArgs {
    /// Stable source id. Default: a new random id.
    #[arg(long)]
    id: Option<String>,
    /// Channel to send on.
    #[arg(long, default_value = ntx::PROGRAM)]
    channel: String,
    /// Also offer the preview channel.
    #[arg(long)]
    preview: bool,
    /// Treat each line as a JSON fields object of --type.
    #[arg(long)]
    json: bool,
    /// Type name used with --json.
    #[arg(long = "type", default_value = "ntx.text")]
    type_name: String,
}

impl LineArgs {
    fn channels(&self) -> Vec<String> {
        let mut channels = vec![ntx::PROGRAM.to_string()];
        if self.preview {
            channels.push(ntx::PREVIEW.to_string());
        }
        channels
    }
}

#[derive(Args)]
struct RelayArgs {
    /// Relay name (the machine name is added in front).
    #[arg(long, default_value = "ntx relay")]
    name: String,
    /// TCP port on 127.0.0.1 (0 = any free port).
    #[arg(long, default_value_t = ntx::DEFAULT_PORT)]
    port: u16,
    /// Require this pairing code from publishers.
    #[arg(long)]
    pair: Option<String>,
    /// Allow web pages from this origin to publish, e.g. `https://example.com`. Repeat for more.
    #[arg(long = "allow-origin")]
    allowed_origins: Vec<String>,
    /// Do not advertise the published sources with mDNS.
    #[arg(long)]
    no_mdns: bool,
}

#[derive(Args)]
struct RecvArgs {
    /// Source address: host, host:port or ws:// URL.
    address: String,
    /// Receiver name (the machine name is added in front).
    #[arg(long, default_value = "ntx recv")]
    name: String,
    /// Channels to receive.
    #[arg(long = "channel", default_values_t = [ntx::PROGRAM.to_string()])]
    channels: Vec<String>,
    /// Pairing code.
    #[arg(long)]
    pair: Option<String>,
    /// Print only this field path, e.g. `text` or `verses.0.text`.
    #[arg(long, conflicts_with = "pattern")]
    field: Option<String>,
    /// Print a pattern, e.g. "{reference} · {version}".
    #[arg(long)]
    pattern: Option<String>,
    /// Report "on air" to the source.
    #[arg(long)]
    on_air: bool,
    /// Exit after this many updates.
    #[arg(long)]
    count: Option<usize>,
}

#[derive(Args)]
struct ListArgs {
    /// How long to look, in seconds.
    #[arg(long, default_value_t = 3)]
    seconds: u64,
}

fn main() -> ExitCode {
    let filter = tracing_subscriber::EnvFilter::try_from_env("NTX_LOG").unwrap_or_else(|_| "warn".into());
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();
    if let Err(e) = ctrlc::set_handler(|| INTERRUPTED.store(true, Ordering::Relaxed)) {
        eprintln!("warning: Ctrl-C will not stop cleanly: {e}");
    }
    let result = match Cli::parse().command {
        Command::Send(args) => send(&args),
        Command::Recv(args) => recv(&args),
        Command::List(args) => list(&args),
        Command::Publish(args) => publish(&args),
        Command::Relay(args) => relay(&args),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn send(args: &SendArgs) -> CliResult {
    let mut config = SourceConfig::new(display_name(&args.name));
    if let Some(id) = &args.lines.id {
        config.id.clone_from(id);
    }
    config.port = args.port;
    config.pairing_code.clone_from(&args.pair);
    config.advertise = !args.no_mdns;
    config.channels = args.lines.channels();
    let source = Source::start(config)?;
    let identity = source.identity();
    eprintln!(
        "source \"{}\" listening on ws://{}/ (id {})",
        identity.name,
        source.local_addr(),
        identity.id
    );
    let describe = |event| match event {
        SourceEvent::ReceiverConnected {
            connection,
            receiver,
            address,
        } => format!("+ receiver #{connection} \"{}\" from {address}", receiver.name),
        SourceEvent::ReceiverDisconnected { connection } => format!("- receiver #{connection}"),
        SourceEvent::TallyChanged(tally) => describe_tally(tally),
    };
    reporting(
        |t| source.wait_event(t),
        describe,
        || send_lines(&args.lines, &source),
    )
}

fn publish(args: &PublishArgs) -> CliResult {
    let mut config = PublisherConfig::new(&args.name);
    if let Some(id) = &args.lines.id {
        config.id.clone_from(id);
    }
    config.pairing_code.clone_from(&args.pair);
    config.channels = args.lines.channels();
    let publisher = Publisher::connect(&args.relay, config)?;
    eprintln!("publishing to {}", publisher.url());
    let describe = |event| match event {
        PublisherEvent::Connected { relay, .. } => format!("connected to relay \"{}\"", relay.name),
        PublisherEvent::Tally(tally) => describe_tally(tally),
        PublisherEvent::Error { code, message } => {
            let error = format!("relay error: {} {}", code.as_str(), message.unwrap_or_default());
            // The publisher stopped (clause 11.3). Reading standard input may block, so exit
            // from here, like `recv` does.
            if code.needs_user() {
                eprintln!("{error}");
                std::process::exit(1);
            }
            error
        }
        PublisherEvent::Disconnected { reason } => format!("disconnected: {reason}"),
    };
    reporting(
        |t| publisher.wait_event(t),
        describe,
        || send_lines(&args.lines, &publisher),
    )
}

fn relay(args: &RelayArgs) -> CliResult {
    let mut config = RelayConfig::new(display_name(&args.name));
    config.port = args.port;
    config.pairing_code.clone_from(&args.pair);
    config.allowed_origins.clone_from(&args.allowed_origins);
    config.advertise = !args.no_mdns;
    let path = config.path.clone();
    let relay = Relay::start(config)?;
    eprintln!("relay listening on ws://{}{path}", relay.local_addr());
    eprintln!("Ctrl-D stops the relay");
    let describe = |event| match event {
        RelayEvent::OriginRequested { origin } => {
            format!("refused a page from {origin} (to allow it, restart with --allow-origin {origin})")
        }
        RelayEvent::PublisherConnected {
            connection,
            source,
            address,
            port,
            ..
        } => format!(
            "+ publisher #{connection} from {address}: serving \"{}\" at ws://127.0.0.1:{port}/",
            source.name
        ),
        RelayEvent::PublisherDisconnected { connection } => format!("- publisher #{connection}"),
    };
    reporting(
        |t| relay.wait_event(t),
        describe,
        || {
            for line in input_lines() {
                line?;
            }
            Ok(())
        },
    )
}

fn describe_tally(tally: Tally) -> String {
    format!("tally: on air {}, preview {}", tally.program, tally.preview)
}

/// Runs `body` while another thread prints the events that `wait` returns.
fn reporting<E>(
    wait: impl Fn(Duration) -> Option<E> + Sync,
    describe: impl Fn(E) -> String + Sync,
    body: impl FnOnce() -> CliResult,
) -> CliResult {
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                if let Some(event) = wait(Duration::from_millis(100)) {
                    eprintln!("{}", describe(event));
                }
            }
        });
        let result = body();
        done.store(true, Ordering::Relaxed);
        result
    })
}

/// What `send` and `publish` send lines with: a source or a publisher.
trait Lines {
    fn update(&self, channel: &str, type_name: &str, fields: Fields) -> ntx::Result<u64>;
    fn update_text(&self, channel: &str, text: &str) -> ntx::Result<u64>;
    fn clear(&self, channel: &str) -> ntx::Result<u64>;
}

macro_rules! lines {
    ($($sender:ty),*) => {$(
        impl Lines for $sender {
            fn update(&self, channel: &str, type_name: &str, fields: Fields) -> ntx::Result<u64> {
                <$sender>::update(self, channel, type_name, fields)
            }
            fn update_text(&self, channel: &str, text: &str) -> ntx::Result<u64> {
                <$sender>::update_text(self, channel, text)
            }
            fn clear(&self, channel: &str) -> ntx::Result<u64> {
                <$sender>::clear(self, channel)
            }
        }
    )*};
}

lines!(Source, Publisher);

/// Sends each line of standard input with `sender`. A line that cannot be sent, such as text
/// over the size limit or invalid JSON, is reported and skipped: receivers keep their source.
fn send_lines(args: &LineArgs, sender: &impl Lines) -> CliResult {
    eprintln!("type a line and press Enter to send it; an empty line clears; Ctrl-D quits");
    for line in input_lines() {
        let line = line?;
        let sent: Result<u64, Box<dyn Error>> = if line.is_empty() {
            sender.clear(&args.channel).map_err(Into::into)
        } else if args.json {
            serde_json::from_str(&line)
                .map_err(Into::into)
                .and_then(|fields| Ok(sender.update(&args.channel, &args.type_name, fields)?))
        } else {
            sender.update_text(&args.channel, &line).map_err(Into::into)
        };
        match sent {
            Ok(seq) => eprintln!("sent seq {seq}"),
            Err(e) => eprintln!("not sent: {e}"),
        }
    }
    Ok(())
}

/// The lines of standard input, until it ends or Ctrl-C is pressed.
fn input_lines() -> impl Iterator<Item = std::io::Result<String>> {
    let (lines, rx) = mpsc::channel();
    // Reading blocks, so it runs on its own thread; the process exits without waiting for it.
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    std::iter::from_fn(move || {
        while !interrupted() {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => return Some(line),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
        None
    })
}

fn recv(args: &RecvArgs) -> CliResult {
    let pattern = args.pattern.as_deref().map(Pattern::parse).transpose()?;
    let mut config = ReceiverConfig::new(display_name(&args.name));
    config.channels.clone_from(&args.channels);
    config.pairing_code.clone_from(&args.pair);
    let receiver = Receiver::connect(&args.address, config)?;
    eprintln!("connecting to {}", receiver.url());
    if args.on_air {
        receiver.set_tally(Tally {
            program: true,
            preview: false,
        });
    }
    let mut updates = 0;
    while args.count.is_none_or(|count| updates < count) && !interrupted() {
        match receiver.wait_event(Duration::from_millis(100)) {
            Some(ReceiverEvent::Connected {
                source,
                session,
                channels,
            }) => {
                eprintln!(
                    "connected to \"{}\" (session {session}, channels {})",
                    source.name,
                    channels.join(", ")
                );
            }
            Some(ReceiverEvent::Announce(types)) => {
                let names: Vec<_> = types.iter().map(|t| t.ntx_type.as_str()).collect();
                eprintln!("announced types: {}", names.join(", "));
            }
            Some(ReceiverEvent::Update(update)) => {
                updates += 1;
                let line = match (&args.field, &pattern) {
                    (Some(path), _) => field_text(&update.fields, path),
                    (None, Some(pattern)) => pattern.render(&update.fields),
                    (None, None) => serde_json::to_string(&update)?,
                };
                println!("{line}");
            }
            Some(ReceiverEvent::Error { code, message }) => {
                let error = format!("source error: {} {}", code.as_str(), message.unwrap_or_default());
                // Trying again cannot help until the settings change (clause 11.3).
                if code.needs_user() {
                    return Err(error.into());
                }
                eprintln!("{error}");
            }
            Some(ReceiverEvent::Disconnected { reason }) => eprintln!("disconnected: {reason}"),
            None => {}
        }
    }
    Ok(())
}

fn list(args: &ListArgs) -> CliResult {
    let browser = Browser::start()?;
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let mut found = 0;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match browser.wait_event(left) {
            Some(BrowserEvent::Found(s)) => {
                found += 1;
                let pair = if s.pairing_required {
                    "  (pairing code needed)"
                } else {
                    ""
                };
                println!("{}  {}  NTX {}  id {}{pair}", s.name, s.url, s.version, s.id);
            }
            Some(BrowserEvent::Lost { name, .. }) => println!("gone: {name}"),
            None => break,
        }
    }
    if found == 0 {
        eprintln!("no NTX sources found (mDNS does not cross subnets or VLANs; try `ntx recv <ip>`)");
    }
    Ok(())
}
