use crate::commands::request::CommandDecodingError;
use crate::mem_table::mem_table::MemTable;
use crate::shards::router::ShardRouter;
use crate::tcp::decoder::TcpCommandDecoder;
use crate::tcp::sender::TcpCommandSender;
use glommio_ng::net::{Preallocated, TcpListener, TcpStream};
use glommio_ng::{Latency, Shares};
use std::rc::Rc;

pub struct Listener {}

impl Listener {
    const PORT: u32 = 9470;

    pub fn listen(
        cpu_shard_id: u32,
        shard_router: ShardRouter<impl MemTable>,
        listen_shares: Shares,
        listen_latency: Latency,
        request_shares: Shares,
        request_latency: Latency,
    ) {
        let shard_port = Self::PORT + cpu_shard_id - 1;
        let tcp_listener = TcpListener::bind(format!("0.0.0.0:{shard_port}"))
            .expect("failed to bind TCP listener");

        let queue_name = format!("tcp-listener-{cpu_shard_id}");
        let connection_queue = glommio_ng::executor().create_task_queue(
            listen_shares,
            listen_latency,
            queue_name.as_str(),
        );

        glommio_ng::spawn_local_into(
            Self::accept_connection(
                tcp_listener,
                shard_router,
                cpu_shard_id,
                request_shares,
                request_latency,
            ),
            connection_queue,
        )
        .expect("failed to spawn tcp listener onto its task queue")
        .detach();
    }

    async fn accept_connection(
        listener: TcpListener,
        router: ShardRouter<impl MemTable>,
        cpu_shard_id: u32,
        shares: Shares,
        latency: Latency,
    ) {
        let requests_queue = glommio_ng::executor().create_task_queue(
            shares,
            latency,
            format!("requests-{cpu_shard_id}").as_str(),
        );

        let router_rc = Rc::new(router);
        loop {
            match listener.accept().await {
                Ok(stream) => {
                    glommio_ng::spawn_local_into(
                        Self::handle_connection(stream.buffered(), Rc::clone(&router_rc)),
                        requests_queue,
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
        mut stream: TcpStream<Preallocated>,
        router: Rc<ShardRouter<impl MemTable>>,
    ) {
        loop {
            let command = match TcpCommandDecoder::tcp_decode(&mut stream).await {
                Ok(command) => command,
                Err(CommandDecodingError::InvalidInput) => break,
                Err(_) => break,
            };

            if let Some(cmd) = command {
                let result = router.dispatch(cmd).await;
                match result {
                    Ok(cmd_result) => {
                        let _ = TcpCommandSender::send(&mut stream, cmd_result).await;
                        // TODO: handle send result error
                    }
                    Err(e) => {
                        // TODO: handle error
                        break; // for now just drop connection
                    }
                }
            } else {
                break; // connection closed
            }
        }
    }
}
