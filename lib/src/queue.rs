use core::panic;
use std::{collections::{HashMap, VecDeque}, time::{self, UNIX_EPOCH}};

use tokio::sync::{mpsc, oneshot};

const MAX_BATCH_BYTE_SIZE: usize = 10_000_000; // 10 MB

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub struct QueueItem {
    key: String,
    value: Vec<u8>,
    created: u64,
}

struct ConsumerGroup {
    topic: String,
    offset: usize,
}

pub struct QueueServer {
    topics: HashMap<String, VecDeque<QueueItem>>,
    consumers: HashMap<String, ConsumerGroup>,
    channel: mpsc::Receiver<UserEvent>,
}

impl QueueServer {
    pub fn new() -> (Self, mpsc::Sender<UserEvent>) {
        let (tx, rx) = mpsc::channel(100);
        return (Self {
            topics: HashMap::new(),
            consumers: HashMap::new(),
            channel: rx,
        }, tx);
    }

    pub async fn init(mut self) {
        loop {
            let event = self.channel.recv().await;

            if event.is_none() {
                break;
            }

            let event = event.unwrap();
            match event.operation {
                QueueOperation::AddTopic(topic) => {
                    self.add_topic(topic);
                    if let Err(_) = event.response_channel.send(None) {
                        print!("Failed to add topic")
                    }
                },
                QueueOperation::AddConsumer(consumer, topic, offset) => {
                    self.add_consumer(consumer, topic, offset);
                    if let Err(_) = event.response_channel.send(None) {
                        print!("Failed to add consumer")
                    }
                },
                QueueOperation::AddItem(topic, key, value) => {
                    self.add_item(topic, key, value);
                    if let Err(_) = event.response_channel.send(None) {
                        print!("Failed to add item")
                    }
                },
                QueueOperation::SetReadOffset(consumer, topic, offset) => {
                    self.set_read_offset(consumer, topic, offset);
                    if let Err(_) = event.response_channel.send(None) {
                        print!("Failed to set read offset")
                    }
                },
                QueueOperation::ReadBatch(consumer, topic, offset) => {
                    let items = self.read_batch(consumer, topic, offset);

                    if let Err(_) = event.response_channel.send(Some(items)) {
                        print!("Failed to read batch")
                    }
                },
            }
        }
    }

    pub fn add_topic(&mut self, topic: String) {
        println!("Added topic: {}", topic);
        self.topics.insert(topic, VecDeque::new());
    }

    pub fn add_consumer(&mut self, consumer: String, topic: String, offset: Option<usize>) {
        self.consumers.insert(consumer, ConsumerGroup {
            topic,
            offset: offset.unwrap_or(0),
        });
    }

    pub fn add_item(&mut self, topic: String, key: String, value: Vec<u8>) {
        println!("Adding item: {}, {:?}", key, value);
        self.topics.get_mut(&topic).unwrap().push_back(QueueItem {
            key,
            value,
            created: time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        });
    }

    pub fn set_read_offset(&mut self, consumer: String, topic: String, offset: usize) {
        if topic == self.consumers.get(&consumer).unwrap().topic {
            self.consumers.get_mut(&consumer).unwrap().offset = offset;
            return
        }

        panic!("Invlid consumer otpic ");
    }

    pub fn read_batch(&mut self, _: String, topic: String, offset: usize) -> Vec<QueueItem>  {
        println!("searching for topic: {}, in: {:?}", topic, self.topics);
        let existing_messages = self.topics.get(&topic).unwrap();
        let mut i = offset;
        let mut batch_size = 0;

        let mut batch = Vec::new();

        while i < self.topics.get(&topic).unwrap().len() && batch_size < MAX_BATCH_BYTE_SIZE {
            println!("getting messge {i}, {:?}", existing_messages);
            let msg = existing_messages.get(i).unwrap();
            batch_size += msg.value.len();
            i += 1;

            batch.push(msg.to_owned())
        }

        return batch;
    }
}

pub struct UserEvent {
    pub operation: QueueOperation,
    pub response_channel: oneshot::Sender<Option<Vec<QueueItem>>>,
}

#[derive(Clone, Debug, bitcode::Encode, bitcode::Decode)]
pub enum QueueOperation {
    AddTopic(String),
    AddConsumer(String, String, Option<usize>),
    AddItem(String, String, Vec<u8>),
    SetReadOffset(String, String, usize),
    ReadBatch(String, String, usize),
}

impl From<String> for  QueueOperation {
    fn from(s: String) -> QueueOperation {
        let mut words = s.split(",");
        match words.next().unwrap() {
            "AddTopic" => QueueOperation::AddTopic(words.next().unwrap().to_owned()),
            "AddConsumer" => QueueOperation::AddConsumer(
                words.next().unwrap().to_owned(),
                words.next().unwrap().to_owned(),
                words.next().unwrap().to_owned().parse().ok(),
            ),
            "AddItem" => {
                QueueOperation::AddItem(
                    words.next().unwrap().to_owned(),
                    words.next().unwrap().to_owned(),
                    words.fold(String::new(), |acc, s| acc + s).as_bytes().to_owned(),
                )
            },
            "SetReadOffset" => QueueOperation::SetReadOffset(words.next().unwrap().to_owned(), words.next().unwrap().to_owned(), words.next().unwrap().parse().unwrap()),
            "ReadBatch" => QueueOperation::ReadBatch(words.next().unwrap().to_owned(), words.next().unwrap().to_owned(), words.next().unwrap().parse().unwrap()),
            _ => panic!("Invalid operation"),
        }
    }
}
