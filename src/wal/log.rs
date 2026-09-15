use crate::commands::request::CommandRequest;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalError;
use crate::wal::mode::WalMode;
use crate::wal::writer::WalWriter;
use futures::StreamExt;
use futures::future::Either;
use glommio_ng::channels::local_channel;
use glommio_ng::channels::local_channel::{LocalReceiver, LocalSender};
use glommio_ng::io::DmaFile;
use glommio_ng::{Latency, Shares};
use std::path::Path;
use std::rc::Rc;

type WalReply = LocalSender<Result<(), Rc<WalError>>>;

pub struct WalCommand {
    req: CommandRequest,
    reply: WalReply,
}

#[derive(Clone)]
pub struct Wal {
    cpu_shard_id: u32,
    sender: Rc<LocalSender<WalCommand>>,
}

impl Wal {
    pub async fn init(
        cpu_shard_id: u32,
        mode: WalMode,
        dir: &Path,
        storage_config: Rc<StorageConfig>,
        shares: Shares,
        latency: Latency,
    ) -> Result<Self, WalError> {
        let path = dir.join(format!("WAL_{cpu_shard_id}"));
        DmaFile::create(&path)
            .await
            .map_err(|e| WalError::CreateFailed(e.into()))?;

        let mode = Rc::new(mode);
        let writer = WalWriter::init(&path, Rc::clone(&mode), storage_config)
            .await
            .map_err(WalError::WriterFailed)?;

        let queue_name = format!("wal-{}", cpu_shard_id);
        let task_queue =
            glommio_ng::executor().create_task_queue(shares, latency, queue_name.as_str());

        let (sender, receiver) = local_channel::new_unbounded();
        glommio_ng::spawn_local_into(Self::run(writer, mode, receiver), task_queue)
            .expect("could not spawn WAL onto its task queue")
            .detach();

        Ok(Wal {
            cpu_shard_id,
            sender: Rc::new(sender),
        })
    }

    async fn run(mut writer: WalWriter, mode: Rc<WalMode>, receiver: LocalReceiver<WalCommand>) {
        // Like in manifest, we don't retry here. Let the caller do it.
        let mut commands = receiver.stream();
        let (batch_window, max_batch_size) = match *mode {
            WalMode::Strict {
                batch_window,
                max_batch_size,
            }
            | WalMode::Relaxed {
                batch_window,
                max_batch_size,
            } => (batch_window, max_batch_size),
        };

        if batch_window.is_zero() {
            panic!("batch window cannot be zero (has no meaning and causes starvation)");
        }

        let mut deadline = glommio_ng::timer::Timer::new(batch_window);
        let mut replies = Vec::with_capacity(max_batch_size);
        let mut in_batch_processed = 0usize;
        loop {
            match futures::future::select(commands.next(), &mut deadline).await {
                Either::Left((Some(cmd), _)) => {
                    let result = writer
                        .append(&cmd.req)
                        .await
                        .map_err(|e| Rc::new(WalError::WriterFailed(e)));

                    match mode.as_ref() {
                        WalMode::Strict { .. } => {
                            // Individual write error, return immediately
                            if result.is_err() {
                                let _ = cmd.reply.try_send(result); // caller may have gone
                                continue;
                            }

                            // Write succeeded, but batch fsync may fail
                            replies.push(cmd.reply);

                            if replies.len() >= max_batch_size {
                                Self::strict_batch_fsync(&mut writer, &replies).await;
                                replies.clear();
                                deadline.reset(batch_window);
                            }
                        }
                        WalMode::Relaxed { .. } => {
                            // Send result immediately
                            let _ = cmd.reply.try_send(result); // caller may have gone
                            in_batch_processed += 1;

                            if in_batch_processed >= max_batch_size {
                                Self::relaxed_batch_fsync(&mut writer).await;
                                in_batch_processed = 0;
                                deadline.reset(batch_window);
                            }
                        }
                    }
                }

                Either::Left((None, _)) => break, // channel closed

                Either::Right(_) => match mode.as_ref() {
                    WalMode::Strict { .. } => {
                        if !replies.is_empty() {
                            Self::strict_batch_fsync(&mut writer, &replies).await;
                            replies.clear();
                        }

                        deadline.reset(batch_window);
                    }
                    WalMode::Relaxed { .. } => {
                        if in_batch_processed > 0 {
                            Self::relaxed_batch_fsync(&mut writer).await;
                            in_batch_processed = 0;
                        }

                        deadline.reset(batch_window);
                    }
                },
            }
        }

        let _ = writer.finish().await;
    }

    async fn strict_batch_fsync(writer: &mut WalWriter, replies: &[WalReply]) {
        let fsync_result = writer
            .fsync()
            .await
            .map_err(|e| Rc::new(WalError::WriterFailed(e)));
        if let Err(e) = fsync_result {
            // We fail fast to discard possibly corrupted write while reading WAL. (will be possible after implementing checksums)
            panic!("Could not guarantee durability - WAL fsync failed: {:?}", e);
        } else {
            for reply in replies {
                let _ = reply.try_send(Ok(()));
            }
        }
    }

    async fn relaxed_batch_fsync(writer: &mut WalWriter) {
        let result = writer.fsync().await;
        if let Err(e) = result {
            // We fail fast to discard possibly corrupted write while reading WAL. (will be possible after implementing checksums)
            panic!("Could not guarantee durability - WAL fsync failed: {:?}", e);
        }
    }

    pub async fn append(&self, request: CommandRequest) -> Result<(), Rc<WalError>> {
        let (reply_tx, reply_rx) = local_channel::new_bounded(1);
        self.sender
            .try_send(WalCommand {
                req: request,
                reply: reply_tx,
            })
            .map_err(|_| Rc::new(WalError::ActorGone))?;

        reply_rx
            .stream()
            .next()
            .await
            .ok_or(Rc::new(WalError::ActorGone))?
    }
}
