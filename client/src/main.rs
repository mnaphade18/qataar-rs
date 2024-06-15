use std::{io::{Read, Write}, net::TcpStream, thread::sleep, time::Duration};
use lib::queue::{QueueItem, QueueOperation};
use bitcode;

fn main() {
    println!("Sending connection req..");
    let mut s = TcpStream::connect("127.0.0.1:8020").unwrap();

    read(&mut s);
    read(&mut s);


    s.write(b"aaa,bbb\n").unwrap();
    println!("Write done {:?} \\n is : {:?}", b"aaa,bbb", b"\n");

    read(&mut s);


    loop {
        let line = read_user_line();
        println!("GOT LINE: {line}.");
        if let Some(operation) = convert_to_operation(&line) {

            let encoded_data = bitcode::encode(&operation);
            s.write(&encoded_data).unwrap_or_else(|err| { println!("Failed to write opeation, {:?}, Err: {:?}", operation, err); return 0; });

            match operation {
                QueueOperation::ReadBatch(_,_,_) => {
                    let buf = read_bytes(&s);
                    println!("Got buf: {buf:?}");

                    let res: Result<Vec<QueueItem>,_> = bitcode::decode(&buf);

                    println!("Got res: {res:?}");
                }
                _ => ()
            };
        } else {
            println!("Invalid user input received: {}. Cannot create operation", line);
        }
    }
}

fn convert_to_operation(line: &str) -> Option<QueueOperation> {
    let mut iter = line.split(',');

    match iter.next() {
        Some(command) => {
            match command {
                "add-topic" => {
                    if let Some(topic) = iter.next() {
                        return Some(QueueOperation::AddTopic(topic.to_owned()));
                    } else {
                        return None;
                    }
                }
                "add-consumer" => {
                    let consumer = iter.next().unwrap().to_owned();
                    let topic = iter.next().unwrap().to_owned();
                    let offset = iter.next().map(|d| d.parse::<usize>().unwrap());

                    return Some(QueueOperation::AddConsumer(consumer, topic, offset))
                }
                "add-item" => {
                    let topic = iter.next().unwrap().to_owned();
                    let key = iter.next().unwrap().to_owned();
                    let value = iter.next().unwrap().as_bytes().to_vec();

                    return Some(QueueOperation::AddItem(topic, key, value));
                }
                "read-batch" => {
                    let consumer = iter.next().unwrap().to_owned();
                    let topic = iter.next().unwrap().to_owned();
                    let offset = iter.next().unwrap().parse().unwrap();

                    return Some(QueueOperation::ReadBatch(consumer, topic, offset));
                }
                _ => { return None; }
            };
        },
        None => {
            return None;
        },
    };
}

fn read_bytes(mut s: &TcpStream) -> Vec<u8> {
    let mut buf = vec![0;1024];
    let sz = s.read(&mut buf).unwrap();

    println!("Read got size: {sz}");

    return buf[..sz].to_owned();
}
fn read(mut s: &TcpStream) {
    let buf = read_bytes(&mut s);

    println!("{}", String::from_utf8_lossy(&buf));
}

fn read_user_line() -> String {
    println!("Please enter operation:");
    let mut input = String::new();

    std::io::stdin().read_line(&mut input).unwrap();

    return input.trim().to_owned();
}
