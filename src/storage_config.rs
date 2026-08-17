pub struct StorageConfig {
    pub memory_table_bytes_max_size: u32,
    pub ss_table_flush_buffer_size_bytes: usize,
    pub ss_table_flush_buffer_writes_behind: usize,
    pub ss_table_compaction_buffer_size_bytes: usize,
    pub ss_table_compaction_buffer_writes_behind: usize,
    pub ss_table_compaction_yield_check_processed_threshold: u32,
    pub ss_table_level_1_target_size_bytes: u32,
    pub ss_table_level_0_target_tables_count: u8,
    pub ss_table_level_growth_factor: u8,
    pub ss_table_read_buffer_size: u32,
    pub ss_table_buffer_read_ahead: u32,
}
