//! The text a source or publisher holds: the current item of each channel and the announced
//! types (clauses 9 and 15.5).

use std::collections::HashMap;

use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::protocol::{Announce, Fields, MAX_SEQ, Message, TypeDescriptor, Update, check_type_name};
use crate::wire::{self, frame};
use crate::{Error, PROGRAM, Result};

pub(crate) struct Store {
    channels: Vec<String>,
    seq: u64,
    /// Current item of each channel, with the sequence number of its last change.
    items: HashMap<String, (u64, String, Fields)>,
    types: Option<Vec<TypeDescriptor>>,
}

impl Store {
    /// An empty store. `channels` must be valid (clause 18.4) and include `program`.
    pub(crate) fn new(channels: Vec<String>) -> Result<Store> {
        crate::protocol::check_channels(&channels, false)?;
        if !channels.iter().any(|c| c == PROGRAM) {
            return Err(Error::UnknownChannel(PROGRAM.to_string()));
        }
        Ok(Store {
            channels,
            seq: 0,
            items: HashMap::new(),
            types: None,
        })
    }

    pub(crate) fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Replaces the item of a channel (clause 9.3), normalising its strings (clause 17).
    /// Returns the sequence number and the `update` to send.
    pub(crate) fn update(
        &mut self,
        channel: &str,
        type_name: &str,
        fields: Fields,
    ) -> Result<(u64, WsMessage)> {
        if !self.channels.iter().any(|c| c == channel) {
            return Err(Error::UnknownChannel(channel.to_string()));
        }
        check_type_name(type_name)?;
        if self.seq >= MAX_SEQ {
            return Err(Error::InvalidMessage(
                "sequence numbers exhausted; restart the source".into(),
            ));
        }
        let update = Update {
            channel: channel.to_string(),
            seq: self.seq + 1,
            type_name: type_name.to_string(),
            fields: wire::normalize_fields(fields),
        };
        let frame = frame(&Message::Update(update.clone()))?;
        self.seq = update.seq;
        self.items
            .insert(update.channel, (update.seq, update.type_name, update.fields));
        Ok((self.seq, frame))
    }

    /// Sets the announced types (clause 15.5). Returns the `announce` to send.
    pub(crate) fn announce(&mut self, types: Vec<TypeDescriptor>) -> Result<WsMessage> {
        for t in &types {
            t.validate()?;
        }
        let frame = frame(&Message::Announce(Announce { types: types.clone() }))?;
        self.types = Some(types);
        Ok(frame)
    }

    /// The announcement, if any, and the current item of each of `channels` (clause 9.2).
    pub(crate) fn state(&self, channels: &[String]) -> Vec<WsMessage> {
        let announce = self
            .types
            .iter()
            .map(|types| Message::Announce(Announce { types: types.clone() }));
        let updates = channels.iter().map(|channel| {
            // Carries the sequence number of the item's last change, 0 if never set.
            let (seq, type_name, fields) = self
                .items
                .get(channel)
                .cloned()
                .unwrap_or_else(|| (0, "ntx.text".to_string(), Fields::new()));
            Message::Update(Update {
                channel: channel.clone(),
                seq,
                type_name,
                fields,
            })
        });
        announce
            .chain(updates)
            .map(|m| WsMessage::text(m.to_json()))
            .collect()
    }
}

/// The fields of an `ntx.text` item.
pub(crate) fn text_fields(text: &str) -> Fields {
    let mut fields = Fields::new();
    fields.insert("text".into(), Value::String(text.to_string()));
    fields
}

/// `update_text` and `clear`: the shortcuts that sources and publishers offer on top of their
/// `update` method.
macro_rules! text_shortcuts {
    () => {
        /// Sends plain text as an `ntx.text` item.
        ///
        /// # Errors
        ///
        /// As [`Self::update`].
        pub fn update_text(&self, channel: &str, text: &str) -> $crate::Result<u64> {
            self.update(channel, "ntx.text", $crate::store::text_fields(text))
        }

        /// Clears a channel (clause 9.5).
        ///
        /// # Errors
        ///
        /// Fails for an unknown channel.
        pub fn clear(&self, channel: &str) -> $crate::Result<u64> {
            self.update(channel, "ntx.text", $crate::Fields::new())
        }
    };
}

pub(crate) use text_shortcuts;
