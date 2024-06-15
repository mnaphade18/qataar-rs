use std::io;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use crate::error;
use lib::queue::{QueueOperation, UserEvent};

pub struct Server {
    listener: TcpListener,
    channel: mpsc::Sender<UserEvent>
}

impl Server {
    pub async fn new(tx: mpsc::Sender<UserEvent>) -> Server {
        let listener = TcpListener::bind("127.0.0.1:8020").await.unwrap();
        println!("server bind done");
        let s = Server {
            listener,
            channel: tx,
        };

        s.setup().await;

        return s;
    }

    async fn setup(&self) {
        loop {
            let socket = self.listener.accept().await;
            match socket {
                Ok((s, _)) => {
                    println!("new conn accepted");
                    let tx = self.channel.clone();
                    tokio::spawn(async move { handle_stream(s, tx).await });
                },
                Err(e) => println!("Error accepting conn: {}", e),
            };
        }
    }
}

struct ClientConn {
    stream: TcpStream,
    channel: mpsc::Sender<UserEvent>,

}
impl ClientConn {
    pub fn new(stream: TcpStream, channel: mpsc::Sender<UserEvent>) -> Self {
        Self { stream, channel }
    }

    async fn write_bytes(&mut self, bytes: &[u8]) {
        self.stream.write(bytes).await.unwrap();
    }

    async fn write_to_client(&mut self, data: String) {
        self.write_bytes(&data.into_bytes()).await;
        self.stream.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    async fn read_bytes(&mut self) -> Vec<u8> {
        let mut buf = vec![0;1024];
        let size = self.stream.read(&mut buf).await.unwrap();

        return buf[..size].to_owned();
    }
    async fn read_input(&mut self) -> String {
        let b = self.read_bytes().await;

        return String::from_utf8_lossy(&b).trim().to_string();
    }
    async fn accept_auth(&mut self) -> Result<(), error::ServerError> {
        let line = self.read_input().await;
        println!("GOt auth line: {}", line);
        let mut words = line.split(",");

        let username = words.next().unwrap();
        let password = words.next().unwrap();
        println!("got input: {:?}, {:?}", username, password);

        if username == "aaa" && password == "bbb" {
            return Ok(());
        }

        return Err(error::ServerError::UnAuth)
    }
}

async fn handle_stream(stream: TcpStream, channel: mpsc::Sender<UserEvent>) {
    let mut conn = ClientConn::new(stream, channel);

    conn.write_to_client("Connected to server qataar".to_owned()).await;
    conn.write_to_client("Enter username,password (comma separated)".to_owned()).await;

    match conn.accept_auth().await {
        Ok(_) => { 
            println!("User authenticated");
        },
        Err(e) => panic!("Error authenticating user: {}", e),
    }

    conn.write_to_client("Auth done".to_owned()).await;

    loop {
        let input = conn.read_bytes().await;
        if input.len() == 0 {
            let ip = conn.stream.local_addr();
            println!("closed connection: {:?}", ip);
            return;
        }
        match bitcode::decode::<QueueOperation>(&input) {
            Ok(operation) => {
                let (tx, rx)= oneshot::channel();

                let r = conn.channel.send(UserEvent { operation, response_channel: tx}).await;
                if let Err(e) = r {
                    println!("Error sending to channel: {}", e);
                    break;
                }

                if let Some(resp) = rx.await.unwrap() {
                    let bytes_to_write = bitcode::encode(&resp);
                    conn.write_bytes(&bytes_to_write).await;
                }
            }
            Err(er) => {
                println!("Failed to deserialize input from client: {:?}", er);
            }
        } 
    }
}

