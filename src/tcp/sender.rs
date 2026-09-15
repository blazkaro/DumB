use crate::db_entry::DbValue;
use crate::shards::router::CommandResult;
use crate::tcp::listener::TcpCommandResult;
use futures::AsyncWriteExt;
use futures::io::WriteHalf;
use glommio_ng::GlommioError;
use glommio_ng::net::{Preallocated, TcpStream};

pub struct TcpCommandSender {}

impl TcpCommandSender {
    pub async fn send(
        stream: &mut WriteHalf<TcpStream<Preallocated>>,
        result: TcpCommandResult,
    ) -> Result<(), GlommioError<()>> {
        stream.write_all(&result.req_id.to_le_bytes()).await?; // Request id
        match result.result {
            CommandResult::Ack => {
                stream.write_all(&[0u8]).await?; // Ack - 0
            }
            CommandResult::Value(value_opt) => {
                stream.write_all(&[1u8]).await?; // Returns value - 1

                if let Some(value) = value_opt
                    && *value != DbValue::Tombstone
                {
                    match value.as_ref() {
                        DbValue::Tombstone => {
                            unreachable!();
                        }
                        DbValue::Value(bytes) => {
                            stream
                                .write_all(&(bytes.len() as u32).to_le_bytes())
                                .await?; // Value length
                            stream.write_all(bytes).await?; // Value
                        }
                    }
                } else {
                    stream.write_all(&0u32.to_le_bytes()).await?; // Value length, 0 bytes
                }
            }
            CommandResult::ShardMismatch => {
                stream.write_all(&[2u8]).await?; // Shard mismatch - 2
            }
        }

        stream.flush().await?;
        Ok(())
    }
}
