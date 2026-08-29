use crate::le_reader::LeReader;
use crate::ss_table::metadata::{SsTableId, SsTableMetadata};
use std::rc::Rc;

pub(super) enum ManifestEntry {
    AddSsTable(Rc<SsTableMetadata>), // 0
    RemoveSsTable(SsTableId),        // 1
}

impl ManifestEntry {
    pub(super) fn encode(&self, buffer: &mut Vec<u8>) {
        match &self {
            ManifestEntry::AddSsTable(metadata) => {
                buffer.push(0u8); // Entry type
                buffer.extend_from_slice(&metadata.id.to_le_bytes()); // Id
                buffer.extend_from_slice(&metadata.entry_count.to_le_bytes()); // Entry count
                buffer.extend_from_slice(&metadata.size_bytes.to_le_bytes()); // Size (bytes count)
                buffer.extend_from_slice(&metadata.level.to_le_bytes()); // Level

                buffer.extend_from_slice(&(metadata.min_key.len() as u32).to_le_bytes()); // Min key len
                buffer.extend_from_slice(&metadata.min_key); // Min key

                buffer.extend_from_slice(&(metadata.max_key.len() as u32).to_le_bytes()); // Max key len
                buffer.extend_from_slice(&metadata.max_key); // Max key
            }
            ManifestEntry::RemoveSsTable(id) => {
                buffer.push(1u8); // Entry type
                buffer.extend_from_slice(&id.to_le_bytes()); // Id
            }
        }
    }

    pub(super) fn decode(buffer: &[u8]) -> Option<(ManifestEntry, u32)> {
        let mut offset: usize = 0;

        let entry_type: u8 = buffer[0];
        offset += 1;

        match entry_type {
            0u8 => {
                let id = LeReader::read_u32_le(buffer, offset);
                offset += size_of::<u32>();

                let entry_count = LeReader::read_u32_le(buffer, offset);
                offset += size_of::<u32>();

                let size_bytes = LeReader::read_u64_le(buffer, offset);
                offset += size_of::<u64>();

                let level = LeReader::read_u16_le(buffer, offset);
                offset += size_of::<u16>();

                let min_key_len = LeReader::read_u32_le(buffer, offset);
                offset += size_of::<u32>();

                let min_key: &[u8] = &buffer[offset..offset + min_key_len as usize];
                offset += min_key_len as usize;

                let max_key_len = LeReader::read_u32_le(buffer, offset);
                offset += size_of::<u32>();

                let max_key: &[u8] = &buffer[offset..offset + max_key_len as usize];
                offset += max_key_len as usize;

                Some((
                    ManifestEntry::AddSsTable(Rc::new(SsTableMetadata {
                        id,
                        entry_count,
                        size_bytes,
                        level,
                        min_key: min_key.to_vec(),
                        max_key: max_key.to_vec(),
                    })),
                    offset as u32,
                ))
            }
            1u8 => {
                let id = LeReader::read_u32_le(buffer, offset);
                offset += size_of::<u32>();
                Some((ManifestEntry::RemoveSsTable(id), offset as u32))
            }
            _ => None,
        }
    }
}
