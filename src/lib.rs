pub const MAX_MEMTABLE: usize = 1 << 2;
pub const FOOTER_SIZE: usize = 1 << 5;
pub const INDEX_DENSITY: usize = 2;

pub mod encryption;
pub mod segment;
pub mod store;
