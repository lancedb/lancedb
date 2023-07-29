use std::sync::mpsc;
use std::thread;

use neon::{prelude::*, types::Deferred};

use crate::error::{Error, Result};

type TableCallback = Box<dyn FnOnce(&mut vectordb::Table, &Channel, Deferred) + Send>;

// Wraps a LanceDB table into a channel, allowing concurrent access
pub(crate) struct JsTable {
    tx: mpsc::Sender<JsTableMessage>,
}

impl Finalize for JsTable {}

// Messages sent on the table channel
pub(crate) enum JsTableMessage {
    // Promise to resolve and callback to be executed
    Callback(Deferred, TableCallback),
}

impl JsTable {
    pub(crate) fn new<'a, C>(cx: &mut C, mut table: vectordb::Table) -> Result<Self>
    where
        C: Context<'a>,
    {
        // Creates a mpsc Channel to receive messages  / commands from Javascript
        let (tx, rx) = mpsc::channel::<JsTableMessage>();
        let channel = cx.channel();

        // Spawn a new thread to receive messages without blocking the main JS thread
        thread::spawn(move || {
            // Runs until the channel is closed
            while let Ok(message) = rx.recv() {
                match message {
                    JsTableMessage::Callback(deferred, f) => {
                        f(&mut table, &channel, deferred);
                    }
                }
            }
        });

        Ok(Self { tx })
    }

    pub(crate) fn send(
        &self,
        deferred: Deferred,
        callback: impl FnOnce(&mut vectordb::Table, &Channel, Deferred) + Send + 'static,
    ) -> Result<()> {
        self.tx
            .send(JsTableMessage::Callback(deferred, Box::new(callback)))
            .map_err(Error::from)
    }
}
