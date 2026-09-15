use crate::commands::get::GetCommand;
use crate::commands::remove::RemoveCommand;
use crate::commands::request::{CommandDecodingError, CommandRequest};
use crate::commands::set::SetCommand;
use crate::db_entry::DbValue;
use crate::le_reader::LeReader;
use futures::AsyncReadExt;
use futures::io::ReadHalf;
use glommio_ng::net::{Preallocated, TcpStream};

pub struct TcpDecodedCommand {
    pub req_id: u16,
    pub req: CommandRequest,
}

pub struct TcpCommandDecoder {}

// Command structure: command type (1 byte), request id (2 bytes), key len (4 bytes), key (x bytes)
// Then, depending on command type, there may be value len (4 bytes), value (y bytes)
impl TcpCommandDecoder {
    pub async fn tcp_decode(
        stream: &mut ReadHalf<TcpStream<Preallocated>>,
    ) -> Result<Option<TcpDecodedCommand>, CommandDecodingError> {
        let mut command_type_bytes = [0u8; 1];
        match stream.read(&mut command_type_bytes).await {
            Ok(0) => return Ok(None),
            Ok(1) => { /* got the byte */ }
            Err(e) => return Err(CommandDecodingError::Error(e)),
            _ => unreachable!(), // read into 1-byte buffer cannot return anything except 0 or 1
        };

        let command_type = command_type_bytes[0];

        let mut req_id_bytes = [0u8; 2];
        stream
            .read_exact(&mut req_id_bytes)
            .await
            .map_err(CommandDecodingError::Error)?;
        let req_id = u16::from_le_bytes(req_id_bytes);

        let mut key_len_bytes = [0u8; 4];
        stream
            .read_exact(&mut key_len_bytes)
            .await
            .map_err(CommandDecodingError::Error)?;
        let key_len = LeReader::read_u32_le(key_len_bytes.as_slice(), 0);

        let mut key = vec![0u8; key_len as usize];
        stream
            .read_exact(&mut key)
            .await
            .map_err(CommandDecodingError::Error)?;

        match command_type {
            0 => {
                let mut val_len_bytes = [0u8; 4];
                stream
                    .read_exact(&mut val_len_bytes)
                    .await
                    .map_err(CommandDecodingError::Error)?;
                let val_len = LeReader::read_u32_le(val_len_bytes.as_slice(), 0);

                let mut value = vec![0u8; val_len as usize];
                stream
                    .read_exact(&mut value)
                    .await
                    .map_err(CommandDecodingError::Error)?;

                Ok(Some(TcpDecodedCommand {
                    req: CommandRequest::Set(SetCommand {
                        key,
                        value: DbValue::Value(value), // We assume client can't use SET to write Tombstone
                    }),
                    req_id,
                }))
            }
            1 => Ok(Some(TcpDecodedCommand {
                req: CommandRequest::Get(GetCommand { key }),
                req_id,
            })),
            2 => Ok(Some(TcpDecodedCommand {
                req: CommandRequest::Remove(RemoveCommand { key }),
                req_id,
            })),
            _ => Err(CommandDecodingError::InvalidInput),
        }
    }
}
