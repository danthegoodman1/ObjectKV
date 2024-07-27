use objectkv::test;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    tokio::spawn(test()).await;
}
