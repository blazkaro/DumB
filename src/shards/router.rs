use crate::commands::handler::CommandHandler;
use crate::commands::request::CommandRequest;
use crate::db_entry::DbValue;
use crate::mem_table::mem_table::MemTable;
use crate::shards::errors::CommandRouterError;
use std::rc::Rc;
use xxhash_rust::xxh3::xxh3_64;

pub enum CommandResult {
    ShardMismatch,
    Ack,
    Value(Option<Rc<DbValue>>),
}

pub struct ShardRouter<MT: MemTable + 'static> {
    command_handler: CommandHandler<MT>,
    cpu_shard_id: u32,
    shards_count: u32,
}

impl<MT: MemTable + 'static> ShardRouter<MT> {
    pub fn new(cpu_shard_id: u32, shards_count: u32, command_handler: CommandHandler<MT>) -> Self {
        Self {
            cpu_shard_id,
            shards_count,
            command_handler,
        }
    }

    pub async fn dispatch(&self, req: CommandRequest) -> Result<CommandResult, CommandRouterError> {
        let key = match &req {
            CommandRequest::Set(cmd) => &cmd.key,
            CommandRequest::Get(cmd) => &cmd.key,
            CommandRequest::Remove(cmd) => &cmd.key,
        };

        // TODO: hash % shards_count makes nightmares real when changing shards count
        let valid_target_cpu_shard = xxh3_64(key) % self.shards_count as u64 + 1; // CPU shard ids start from 1
        if valid_target_cpu_shard == self.cpu_shard_id as u64 {
            self.execute_local(req).await
        } else {
            // Request sent to invalid CPU shard
            // TODO: maybe fallback to cross-shard routing (as an option to configure)
            Ok(CommandResult::ShardMismatch)
        }
    }

    async fn execute_local(
        &self,
        req: CommandRequest,
    ) -> Result<CommandResult, CommandRouterError> {
        match req {
            CommandRequest::Set(cmd) => {
                self.command_handler
                    .set(cmd.key, cmd.value)
                    .map_err(CommandRouterError::SetFailed)?;
                Ok(CommandResult::Ack)
            }

            CommandRequest::Get(cmd) => {
                let value = self
                    .command_handler
                    .get(&cmd.key)
                    .await
                    .map_err(CommandRouterError::GetFailed)?;
                Ok(CommandResult::Value(value))
            }

            CommandRequest::Remove(cmd) => {
                self.command_handler
                    .remove(cmd.key)
                    .map_err(CommandRouterError::RemoveFailed)?;
                Ok(CommandResult::Ack)
            }
        }
    }
}
