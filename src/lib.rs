pub const MAX_MEMTABLE: usize = 1 << 2;
pub const FOOTER_SIZE: usize = 1 << 5;

pub mod encryption;
pub mod segment;
pub mod store;
