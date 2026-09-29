use crate::commands::request::CommandRequest;
use crate::errors::retryable::RetryableError;
use crate::retries::retry::{RetryPolicy, retry_if};
use crate::safe_io::errors::SafeIoInitError;
use crate::storage_config::StorageConfig;
use crate::wal::errors::{WalError, WalReleaseError, WalRotationError};
use crate::wal::id_generator::{OrderedWalId, WalId, WalIdGenerator};
use crate::wal::mode::WalMode;
use crate::wal::writer::WalWriter;
use futures::StreamExt;
use futures::future::Either;
use glommio_ng::channels::local_channel;
use glommio_ng::channels::local_channel::{LocalReceiver, LocalSender};
use glommio_ng::io::{Directory, DmaFile, OpenOptions};
use glommio_ng::{GlommioError, Latency, Shares};
use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

pub struct WalRotationResult {
    pub old_id: WalId,
    pub new_id: WalId,
}

type WalAppendReply = LocalSender<Result<(), Rc<WalError>>>;

pub enum WalCommand {
    Request {
        req: CommandRequest,
        reply: WalAppendReply,
    },

    /// Internal use only, contained here for simplicity
    RotationSync { next_id: OrderedWalId },
}

pub struct Wal {
    cpu_shard_id: u32,
    dir: PathBuf,
    id_generator: RefCell<WalIdGenerator>,
    sender: Rc<LocalSender<WalCommand>>,
    active_id: Cell<WalId>, // every change needs to be synchronized with actor loop - it exists so that writes (which includes WAL rotation) can be completely sync and fast.
}

impl Wal {
    pub async fn init(
        cpu_shard_id: u32,
        mode: WalMode,
        dir: PathBuf,
        mut id_generator: WalIdGenerator,
        storage_config: Rc<StorageConfig>,
        shares: Shares,
        latency: Latency,
    ) -> Result<Self, WalError> {
        Self::preallocate_wal_segments(
            cpu_shard_id,
            &id_generator,
            storage_config.memory_table_bytes_max_size as u64,
            dir.clone(),
        )
        .await
        .expect("Failed to preallocate WAL");

        let (sender, receiver) = local_channel::new_unbounded();
        let queue_name = format!("wal-{}", cpu_shard_id);
        let task_queue =
            glommio_ng::executor().create_task_queue(shares, latency, queue_name.as_str());

        let active_id = id_generator
            .next_id()
            .expect("Couldn't generate WAL id during initialization");

        let active_wal_id = active_id.wal_id;
        glommio_ng::spawn_local_into(
            Self::run(
                cpu_shard_id,
                dir.clone(),
                mode,
                active_id,
                Rc::clone(&storage_config),
                receiver,
            ),
            task_queue,
        )
        .expect("could not spawn WAL onto its task queue")
        .detach();

        Ok(Wal {
            cpu_shard_id,
            dir,
            id_generator: RefCell::new(id_generator),
            sender: Rc::new(sender),
            active_id: Cell::new(active_wal_id),
        })
    }

    pub async fn append(&self, request: CommandRequest) -> Result<(), Rc<WalError>> {
        let (reply_tx, reply_rx) = local_channel::new_bounded(1);
        self.sender
            .try_send(WalCommand::Request {
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

    /// Returns old (active till rotation) WAL id
    pub fn rotate(&self) -> Result<WalId, WalRotationError> {
        let next_id = self.id_generator.borrow_mut().next_id();
        if let Some(id) = next_id {
            let old = self.active_id.replace(id.wal_id);

            self.sender
                .try_send(WalCommand::RotationSync { next_id: id })
                .map_err(|_| WalRotationError::ActorGone)?;

            return Ok(old);
        }

        Err(WalRotationError::ExhaustedPool)
    }

    pub async fn free_up(&self, wal_id: WalId) -> Result<(), WalReleaseError> {
        let path = self
            .dir
            .join(format!("WAL_{}_{}", self.cpu_shard_id, wal_id));
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .truncate(true)
            .dma_open(&path)
            .await
            .map_err(|e| WalReleaseError::TruncationFailed(e.into()))?;

        file.fdatasync()
            .await
            .map_err(|e| WalReleaseError::DurabilityError(e.into()))?;

        let _ = file.close().await;

        self.id_generator.borrow_mut().mark_as_free(wal_id);
        Ok(())
    }

    async fn run(
        cpu_shard_id: u32,
        dir: PathBuf,
        mode: WalMode,
        active_id: OrderedWalId,
        storage_config: Rc<StorageConfig>,
        receiver: LocalReceiver<WalCommand>,
    ) {
        // Like in manifest, we don't retry here. Let the caller do it (except the rotating, which is background job)
        let mut commands = receiver.stream();
        let (batch_window, max_batch_size) = match mode {
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

        let mode = Rc::new(mode);

        let mut writer = match Self::next_wal(
            Rc::new(active_id),
            cpu_shard_id,
            &dir,
            Rc::clone(&mode),
            Rc::clone(&storage_config),
        )
        .await
        {
            Ok(writer) => writer,
            Err(e) => panic!("Failed to initialize WAL {:?}", e),
        };

        let _ = active_id; // don't want to use it later by mistake - it is for initialization only

        let mut deadline = glommio_ng::timer::Timer::new(batch_window);
        let mut replies = Vec::with_capacity(max_batch_size);
        let mut in_batch_processed = 0usize;
        loop {
            match futures::future::select(commands.next(), &mut deadline).await {
                Either::Left((Some(cmd), _)) => {
                    match cmd {
                        WalCommand::Request { req, reply } => {
                            let result = writer
                                .append(&req)
                                .await
                                .map_err(|e| Rc::new(WalError::WriterFailed(e)));

                            match mode.as_ref() {
                                WalMode::Strict { .. } => {
                                    // Individual write error, return immediately
                                    if result.is_err() {
                                        let _ = reply.try_send(result); // caller may have gone
                                        continue;
                                    }

                                    // Write succeeded, but batch fsync may fail
                                    replies.push(reply);

                                    if replies.len() >= max_batch_size {
                                        Self::strict_batch_fsync(&mut writer, &replies).await;
                                        replies.clear();
                                        deadline.reset(batch_window);
                                    }
                                }
                                WalMode::Relaxed { .. } => {
                                    // Send result immediately
                                    let _ = reply.try_send(result); // caller may have gone
                                    in_batch_processed += 1;

                                    if in_batch_processed >= max_batch_size {
                                        Self::relaxed_batch_fsync(&mut writer).await;
                                        in_batch_processed = 0;
                                        deadline.reset(batch_window);
                                    }
                                }
                            }
                        }
                        WalCommand::RotationSync { next_id } => {
                            // Channel is FIFO, so all commands that were meant to be processed using the old WAL, were already processed (doesn't mean they were fscyned)
                            // All commands issued after rotation, will be processed after rotation.

                            // Fsync existing batch
                            Self::batch_fsync(
                                mode.as_ref(),
                                &mut writer,
                                &mut replies,
                                &mut in_batch_processed,
                                &mut deadline,
                                batch_window,
                            )
                            .await;

                            // Close current writer
                            let _ = writer.finish().await; // maybe could not close the writer...
                            // TODO: array containing writers whose finishing needs to be retried (to not waste resources)

                            let next_id = Rc::new(next_id);
                            writer = retry_if(
                                &RetryPolicy {
                                    max_attempts: 3,
                                    base_delay: Duration::from_millis(200),
                                    max_delay: Duration::from_millis(1000),
                                },
                                async || {
                                    Self::next_wal(
                                        Rc::clone(&next_id),
                                        cpu_shard_id,
                                        &dir,
                                        Rc::clone(&mode),
                                        Rc::clone(&storage_config),
                                    )
                                    .await
                                },
                                |e| e.is_retryable(),
                            )
                            .await
                            .expect("Failed to rotate WAL. Could not guarantee durability");
                        }
                    }
                }

                Either::Left((None, _)) => break, // channel closed

                Either::Right(_) => {
                    Self::batch_fsync(
                        mode.as_ref(),
                        &mut writer,
                        &mut replies,
                        &mut in_batch_processed,
                        &mut deadline,
                        batch_window,
                    )
                    .await
                }
            }
        }

        let _ = writer.finish().await;
    }

    async fn preallocate_wal_segments(
        cpu_shard_id: u32,
        id_generator: &WalIdGenerator,
        expected_wal_size_bytes: u64,
        dir: PathBuf,
    ) -> Result<(), GlommioError<()>> {
        for id in id_generator.iter_free() {
            let path = dir.join(format!("WAL_{cpu_shard_id}_{id}"));
            let create_result = DmaFile::create(path).await;
            if let Ok(file) = create_result {
                file.pre_allocate(expected_wal_size_bytes, true).await?;
                file.fdatasync().await?;
            }
        }

        let parent_dir = Directory::open(&dir).await?;
        parent_dir.sync().await?;

        Ok(())
    }

    async fn next_wal(
        id: Rc<OrderedWalId>,
        cpu_shard_id: u32,
        dir: &Path,
        mode: Rc<WalMode>,
        storage_config: Rc<StorageConfig>,
    ) -> Result<WalWriter, SafeIoInitError> {
        let path = dir.join(format!("WAL_{}_{}", cpu_shard_id, id.wal_id));
        let writer =
            WalWriter::open_and_truncate(&path, id, Rc::clone(&mode), Rc::clone(&storage_config))
                .await?;

        Ok(writer)
    }

    async fn batch_fsync(
        mode: &WalMode,
        writer: &mut WalWriter,
        replies: &mut Vec<WalAppendReply>,
        in_batch_processed: &mut usize,
        deadline: &mut glommio_ng::timer::Timer,
        batch_window: Duration,
    ) {
        match mode {
            WalMode::Strict { .. } => {
                if !replies.is_empty() {
                    Self::strict_batch_fsync(writer, &replies).await;
                    replies.clear();
                }

                deadline.reset(batch_window);
            }
            WalMode::Relaxed { .. } => {
                if *in_batch_processed > 0 {
                    Self::relaxed_batch_fsync(writer).await;
                    *in_batch_processed = 0;
                }

                deadline.reset(batch_window);
            }
        }
    }

    async fn strict_batch_fsync(writer: &mut WalWriter, replies: &[WalAppendReply]) {
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
}
