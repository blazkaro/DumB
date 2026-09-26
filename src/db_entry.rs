pub type DbKey = Vec<u8>;

#[derive(PartialEq, Clone, Debug)]
pub enum DbValue {
    Value(Vec<u8>),
    Tombstone,
}

#[derive(Debug)]
pub struct DbEntry {
    pub key: DbKey,
    pub value: DbValue,
}
