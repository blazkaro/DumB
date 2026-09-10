use crate::compaction::errors::CompactionTriggerError;
use crate::errors::classified::{ClassifiedError, classify_glommio_error};
use glommio_ng::channels::local_channel::{LocalReceiver, LocalSender};
use std::rc::Rc;

#[derive(Clone)]
pub struct CompactionTrigger {
    sender: Rc<LocalSender<()>>,
}

impl CompactionTrigger {
    pub fn new() -> (Self, LocalReceiver<()>) {
        let (sender, receiver) = glommio_ng::channels::local_channel::new_bounded(1);
        (
            Self {
                sender: Rc::new(sender),
            },
            receiver,
        )
    }

    pub fn notify(&self) -> Result<(), CompactionTriggerError> {
        if let Err(e) = self.sender.try_send(()) {
            match classify_glommio_error(e) {
                ClassifiedError::ChannelClosed(_) => {
                    return Err(CompactionTriggerError::CompactorGone);
                }
                ClassifiedError::WouldBlock(_) => {} // nothing, notification already pending,
                other => panic!(
                    "unexpected result sending to compaction channel {:?}",
                    other
                ),
            };
        }

        Ok(())
    }
}
