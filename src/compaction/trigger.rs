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

    pub fn notify(&self) {
        let _ = self.sender.try_send(());
    }
}
