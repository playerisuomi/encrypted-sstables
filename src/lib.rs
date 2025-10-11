pub const MAX_MEMTABLE: usize = 1 << 2;
pub const FOOTER_SIZE: usize = 1 << 4;

pub mod segment;
pub mod store;
