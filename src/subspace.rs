use crate::db::{DBOps, DB};

pub struct Subspace<'a> {
    db: &'a DB,
}


impl DBOps for Subspace<'_> {
    fn get(key: &str) -> () {
        todo!()
    }
    
    fn write(key: &str, value: &[u8]) -> () {
        todo!()
    }
}
