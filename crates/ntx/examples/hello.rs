//! Sends one line of text and receives it on the same computer.
//!
//! Run with `cargo run -p ntx --example hello`.

use std::time::Duration;

use ntx::{Receiver, ReceiverConfig, ReceiverEvent, Source, SourceConfig, display_name};

fn main() -> Result<(), ntx::Error> {
    let source = Source::start(SourceConfig::new(display_name("Lyrics")))?;
    source.update_text("program", "Amazing grace, how sweet the sound")?;

    let receiver = Receiver::connect("127.0.0.1", ReceiverConfig::new(display_name("Display")))?;
    while let Some(event) = receiver.wait_event(Duration::from_secs(5)) {
        if let ReceiverEvent::Update(update) = event {
            println!("{}", ntx::binding::field_text(&update.fields, "text"));
            break;
        }
    }
    Ok(())
}
