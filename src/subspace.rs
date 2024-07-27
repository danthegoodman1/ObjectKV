use crate::db::{DBOps, DB};

pub struct Subspace<'a> {
    db: &'a DB,
}

impl DBOps for Subspace<'_> {
    async fn get(&self, key: &str) -> Result<(), ()> {
        todo!()
    }

    async fn write(&self, key: &str, value: &[u8]) -> Result<(), ()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get() {
        let db = &DB {};
        let s = Subspace { db: db };
        let r = s.get("hey").await;
        println!("Result: {:?}", r)
    }
}
