//! `ntx`: a small test sender and receiver for NTX.

use std::error::Error;
use std::io::BufRead;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use ntx::binding::{Pattern, field_text};
use ntx::{
    Browser, BrowserEvent, Fields, Receiver, ReceiverConfig, ReceiverEvent, Source, SourceConfig,
    SourceEvent, Tally,
};

type CliResult = Result<(), Box<dyn Error>>;

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
}

#[derive(Args)]
struct SendArgs {
    /// Source name (the machine name is added in front).
    #[arg(long, default_value = "ntx send")]
    name: String,
    /// Stable source id. Default: a new random id.
    #[arg(long)]
    id: Option<String>,
    /// TCP port (0 = any free port).
    #[arg(long, default_value_t = ntx::DEFAULT_PORT)]
    port: u16,
    /// Channel to send on.
    #[arg(long, default_value = ntx::PROGRAM)]
    channel: String,
    /// Also offer the preview channel.
    #[arg(long)]
    preview: bool,
    /// Require this pairing code.
    #[arg(long)]
    pair: Option<String>,
    /// Treat each line as a JSON fields object of --type.
    #[arg(long)]
    json: bool,
    /// Type name used with --json.
    #[arg(long = "type", default_value = "ntx.text")]
    type_name: String,
    /// Do not advertise with mDNS.
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
    let result = match Cli::parse().command {
        Command::Send(args) => send(&args),
        Command::Recv(args) => recv(&args),
        Command::List(args) => list(&args),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// `MACHINE (name)`, the naming style recommended by the spec (clause 8.2).
fn display_name(name: &str) -> String {
    let host = gethostname::gethostname().to_string_lossy().into_owned();
    let machine = host.split('.').next().unwrap_or(&host).to_uppercase();
    format!("{machine} ({name})")
}

fn send(args: &SendArgs) -> CliResult {
    let mut config = SourceConfig::new(display_name(&args.name));
    if let Some(id) = &args.id {
        config.id.clone_from(id);
    }
    config.port = args.port;
    config.pairing_code.clone_from(&args.pair);
    config.advertise = !args.no_mdns;
    if args.preview {
        config.channels.push(ntx::PREVIEW.to_string());
    }
    let source = Source::start(config)?;
    let identity = source.identity();
    eprintln!(
        "source \"{}\" listening on ws://{}/ (id {})",
        identity.name,
        source.local_addr(),
        identity.id
    );
    eprintln!("type a line and press Enter to send it; an empty line clears; Ctrl-D quits");

    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| report_source_events(&source, &done));
        let result = send_lines(&source, args);
        done.store(true, Ordering::Relaxed);
        result
    })
}

fn report_source_events(source: &Source, done: &AtomicBool) {
    while !done.load(Ordering::Relaxed) {
        match source.wait_event(Duration::from_millis(100)) {
            Some(SourceEvent::ReceiverConnected {
                connection,
                receiver,
                address,
            }) => {
                eprintln!("+ receiver #{connection} \"{}\" from {address}", receiver.name);
            }
            Some(SourceEvent::ReceiverDisconnected { connection }) => eprintln!("- receiver #{connection}"),
            Some(SourceEvent::TallyChanged(tally)) => {
                eprintln!("tally: on air {}, preview {}", tally.program, tally.preview);
            }
            None => {}
        }
    }
}

fn send_lines(source: &Source, args: &SendArgs) -> CliResult {
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        let seq = if line.is_empty() {
            source.clear(&args.channel)?
        } else if args.json {
            let fields: Fields = serde_json::from_str(&line)?;
            source.update(&args.channel, &args.type_name, fields)?
        } else {
            source.update_text(&args.channel, &line)?
        };
        eprintln!("sent seq {seq}");
    }
    Ok(())
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
    while args.count.is_none_or(|count| updates < count) {
        match receiver.wait_event(Duration::from_secs(1)) {
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
                eprintln!("source error: {} {}", code.as_str(), message.unwrap_or_default());
            }
            Some(ReceiverEvent::Disconnected { reason }) => eprintln!("disconnected: {reason} (retrying)"),
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
