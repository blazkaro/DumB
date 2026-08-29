pub struct LeReader {}

impl LeReader {
    pub fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
    }

    pub fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
    }

    pub fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap())
    }
}
