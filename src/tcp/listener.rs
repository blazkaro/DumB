use crate::commands::request::CommandDecodingError;
use crate::mem_table::mem_table::MemTable;
use crate::shards::router::{CommandResult, ShardRouter};
use crate::tcp::decoder::TcpCommandDecoder;
use crate::tcp::sender::TcpCommandSender;
use futures::io::WriteHalf;
use futures::{AsyncReadExt, StreamExt};
use glommio_ng::channels::local_channel::LocalReceiver;
use glommio_ng::net::{Preallocated, TcpListener, TcpStream};
use glommio_ng::{Latency, Shares, TaskQueueHandle};
use std::rc::Rc;

pub struct TcpCommandResult {
    pub result: CommandResult,
    pub req_id: u16,
}

pub struct Listener {}

impl Listener {
    const PORT: u32 = 9470;

    pub fn listen(
        cpu_shard_id: u32,
        shard_router: ShardRouter<impl MemTable>,
        shares: Shares,
        latency: Latency,
    ) {
        let shard_port = Self::PORT + cpu_shard_id - 1;
        let tcp_listener = TcpListener::bind(format!("0.0.0.0:{shard_port}"))
            .expect("failed to bind TCP listener");

        let requests_queue = glommio_ng::executor().create_task_queue(
            shares,
            latency,
            format!("requests-{cpu_shard_id}").as_str(),
        );

        glommio_ng::spawn_local_into(
            Self::accept_connection(tcp_listener, shard_router, requests_queue),
            requests_queue,
        )
        .expect("failed to spawn tcp listener onto its task queue")
        .detach();
    }

    async fn accept_connection(
        listener: TcpListener,
        router: ShardRouter<impl MemTable>,
        task_queue: TaskQueueHandle,
    ) {
        let router_rc = Rc::new(router);
        loop {
            match listener.accept().await {
                Ok(stream) => {
                    glommio_ng::spawn_local_into(
                        Self::handle_connection(
                            stream.buffered(),
                            Rc::clone(&router_rc),
                            task_queue,
                        ),
                        task_queue,
                    )
                    .unwrap()
                    .detach();
                }
                Err(e) => {
                    // Accept failed
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream<Preallocated>,
        router: Rc<ShardRouter<impl MemTable>>,
        task_queue: TaskQueueHandle,
    ) {
        let _ = stream.set_nodelay(true);
        let (mut read_half, write_half) = stream.split();

        let (response_tx, response_rx) =
            glommio_ng::channels::local_channel::new_unbounded::<TcpCommandResult>();
        let response_tx = Rc::new(response_tx);

        // The ONLY task that uses writer half. No concurrent calls on write-half.
        glommio_ng::spawn_local_into(Self::writer_task(write_half, response_rx), task_queue)
            .expect("failed to spawn tcp writer task onto its task queue (probably closed)")
            .detach();

        loop {
            let decoded_opt = match TcpCommandDecoder::tcp_decode(&mut read_half).await {
                Ok(command) => command,
                Err(CommandDecodingError::InvalidInput) => break,
                Err(_) => break,
            };

            if let Some(decoded) = decoded_opt {
                let router = Rc::clone(&router);
                let response_tx = Rc::clone(&response_tx);

                glommio_ng::spawn_local_into(
                    async move {
                        match router.dispatch(decoded.req).await {
                            Ok(cmd_result) => {
                                let _ = response_tx.try_send(TcpCommandResult {
                                    result: cmd_result,
                                    req_id: decoded.req_id,
                                });
                                // TODO: handle send result error
                            }
                            Err(e) => {
                                // TODO: handle error
                            }
                        }
                    },
                    task_queue,
                )
                    .expect("failed to spawn tcp command response handler task onto its task queue (probably closed)")
                    .detach();
            } else {
                break; // connection closed
            }
        }
    }

    async fn writer_task(
        mut write_half: WriteHalf<TcpStream<Preallocated>>,
        response_rx: LocalReceiver<TcpCommandResult>,
    ) {
        let mut results = response_rx.stream();
        while let Some(result) = results.next().await {
            if let Err(e) = TcpCommandSender::send(&mut write_half, result).await {
                // TODO: probably connection dead, handle it
                break;
            }
        }
    }
}
