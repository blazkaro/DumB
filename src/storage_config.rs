pub struct StorageConfig {
    pub memory_table_bytes_max_size: u32,
    pub ss_table_flush_buffer_size_bytes: usize,
    pub ss_table_flush_buffer_writes_behind: usize,
    pub ss_table_level_1_target_size_bytes: u32,
    pub ss_table_level_growth_factor: u8,
}
