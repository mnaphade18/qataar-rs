use server::Server;
use lib::queue;

mod server;
mod error;

#[tokio::main]
async fn main() {
    let (qataar, tx) = queue::QueueServer::new();

    tokio::spawn(async move {
        qataar.init().await;
    });
    Server::new(tx).await;
}
