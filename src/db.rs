use crate::subspace::Subspace;

pub trait DBOps {
    fn get(&self, key: &str) -> impl std::future::Future<Output = Result<(), ()>> + Send;

    fn write(
        &self,
        key: &str,
        value: &[u8],
    ) -> impl std::future::Future<Output = Result<(), ()>> + Send;
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
    async fn get(&self, key: &str) -> Result<(), ()> {
        todo!()
    }

    async fn write(&self, key: &str, value: &[u8]) -> Result<(), ()> {
        todo!()
    }
}
