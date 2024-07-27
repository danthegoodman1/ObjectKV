use crate::subspace::Subspace;

pub trait DBOps {
    fn get(key: &str) -> ();
    fn write(key: &str, value: &[u8]) -> ();
}

pub struct DB {
    // TODO: Write batch
}

impl DB {
    pub fn new() -> Self {
        todo!()
    }

    pub fn subspace(prefix: &str) -> &Subspace {
        todo!()
    }
}

impl DBOps for DB {
    fn get(key: &str) -> () {
        todo!()
    }
    
    fn write(key: &str, value: &[u8]) -> () {
        todo!()
    }
}
