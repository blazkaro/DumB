pub type DbKey = Vec<u8>;

#[derive(PartialEq)]
pub enum DbValue {
    Value(Vec<u8>),
    Tombstone,
}

pub struct DbEntry {
    pub key: DbKey,
    pub value: DbValue,
}
