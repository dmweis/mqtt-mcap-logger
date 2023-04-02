use anyhow::{anyhow, Context};
use clap::Parser;
use jsonschema::JSONSchema;
use mcap::{records::MessageHeader, Channel, Schema, WriteOptions};
use memmap2::Mmap;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Outgoing, Publish, QoS};
use serde_json::json;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::BufWriter,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::mpsc::unbounded_channel;

const MQTT_MAX_PACKET_SIZE: usize = 268435455;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// mcap path
    #[arg(short, long)]
    out_dir: String,
    /// mqtt client id
    #[arg(short, long)]
    client_id: String,
    /// mqtt host
    #[arg(short, long)]
    address: String,
    /// mqtt port
    #[arg(short, long)]
    port: u16,
}

enum MqttUpdate {
    Message(Publish),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut mqttoptions = MqttOptions::new(&args.client_id, &args.address, args.port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // outgoing is default
    mqttoptions.set_max_packet_size(MQTT_MAX_PACKET_SIZE, 10 * 1024);
    println!("Starting MQTT server with options {:?}", mqttoptions);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client.subscribe("#", QoS::AtMostOnce).await.unwrap();
    let client_clone = client.clone();

    let (message_sender, mut message_receiver) = unbounded_channel();

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => match notification {
                    Event::Incoming(Incoming::Publish(publish)) => {
                        if let Err(e) = message_sender.send(MqttUpdate::Message(publish)) {
                            eprintln!("Error sending message {e}");
                        }
                    }
                    Event::Incoming(Incoming::ConnAck(_)) => {
                        client.subscribe("#", QoS::AtMostOnce).await.unwrap();
                    }
                    Event::Outgoing(Outgoing::Disconnect) => {
                        println!("Client disconnected. Shutting down");
                        break;
                    }

                    _ => (),
                },
                Err(e) => {
                    eprintln!("Error processing eventloop notifications {e}");
                }
            }
        }
    });

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        client_clone.disconnect().await.unwrap();
    });

    let mut logger =
        RotatingMcapLogger::new(&args.out_dir).context("Failed to create mcap writer")?;

    while let Some(message) = message_receiver.recv().await {
        match message {
            MqttUpdate::Message(publish) => {
                logger.write_message(&publish.topic, &publish.payload)?;
            }
        }
    }

    logger.finish_existing()?;

    Ok(())
}

pub fn nanos_now() -> u64 {
    mcap::records::system_time_to_nanos(&SystemTime::now())
}

/// 24 hours
const ROTATION_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

struct RotatingMcapLogger<'a> {
    mcap_writer: Option<McapLogger<'a>>,
    last_rotation_time: Instant,
    directory_path: String,
}

impl<'a> RotatingMcapLogger<'a> {
    pub fn new(directory_path: &str) -> anyhow::Result<Self> {
        let mut logger = Self {
            mcap_writer: None,
            last_rotation_time: Instant::now(),
            directory_path: directory_path.to_owned(),
        };
        logger.start_new_file()?;
        Ok(logger)
    }

    pub fn write_message(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
        if self.last_rotation_time.elapsed() > ROTATION_INTERVAL {
            self.start_new_file()?;
        }

        // just in case there isn't one
        if self.mcap_writer.is_none() {
            self.start_new_file()?;
        }

        self.mcap_writer
            .as_mut()
            .unwrap()
            .write_message(topic, payload)
    }

    fn start_new_file(&mut self) -> anyhow::Result<()> {
        if let Some(mut mcap_writer) = self.mcap_writer.take() {
            mcap_writer.finish()?;
        }

        let time = nanos_now();
        let filename = format!("{}_ns.mcap", time);
        let full_path = format!("{}/{}", self.directory_path, filename);

        let mcap_writer = McapLogger::new(&full_path)?;
        self.mcap_writer = Some(mcap_writer);
        self.last_rotation_time = Instant::now();
        Ok(())
    }

    fn finish_existing(&mut self) -> anyhow::Result<()> {
        if let Some(mut mcap_writer) = self.mcap_writer.take() {
            mcap_writer.finish()?;
        }
        Ok(())
    }
}

struct McapLogger<'a> {
    mcap: mcap::Writer<'a, BufWriter<File>>,
    channel_map: HashMap<String, u16>,
    channel_message_counter: HashMap<u16, u32>,
}

// 5MB
const MCAP_CHUNK_SIZE: u64 = 5000000;

const JSONSCHEMA_ENCODING_NAME: &str = "jsonschema";
const JSON_SCHEMA_NAME: &str = "json";

impl<'a> McapLogger<'a> {
    pub fn new(file_path: &str) -> anyhow::Result<Self> {
        let writer_options = WriteOptions::new().chunk_size(Some(MCAP_CHUNK_SIZE));
        let mcap = writer_options.create(BufWriter::new(fs::File::create(file_path)?))?;
        Ok(Self {
            mcap,
            channel_map: HashMap::new(),
            channel_message_counter: HashMap::new(),
        })
    }

    pub fn write_message(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
        let time = nanos_now();
        let channel_id = self.add_channel(topic, payload)?;
        let sequence_index = self
            .channel_message_counter
            .entry(channel_id)
            .and_modify(|counter| *counter += 1)
            .or_insert(0);

        let message_header = MessageHeader {
            channel_id,
            sequence: *sequence_index,
            log_time: time,
            publish_time: time,
        };
        self.mcap.write_to_known_channel(&message_header, payload)?;
        Ok(())
    }

    fn add_channel(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<u16> {
        if let Some(channel_id) = self.channel_map.get(topic) {
            return Ok(*channel_id);
        }
        let channel = if serde_json::from_slice::<serde_json::Value>(payload).is_ok() {
            let schema = Some(Arc::new(Schema {
                name: JSON_SCHEMA_NAME.to_owned(),
                encoding: JSONSCHEMA_ENCODING_NAME.to_owned(),
                // json schema that accepts anything
                // https://json-schema.org/understanding-json-schema/basics.html#id1
                // except not json. Like plain text without quotes
                data: Cow::from(String::from("{}").as_bytes().to_vec()),
            }));

            Channel {
                topic: topic.to_owned(),
                schema,
                message_encoding: JSONSCHEMA_ENCODING_NAME.to_owned(),
                metadata: BTreeMap::default(),
            }
        } else {
            Channel {
                topic: topic.to_owned(),
                schema: None,
                message_encoding: String::from(""),
                metadata: BTreeMap::default(),
            }
        };

        let channel_id = self.mcap.add_channel(&channel)?;
        self.channel_map.insert(topic.to_owned(), channel_id);
        Ok(channel_id)
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.mcap.finish()?;
        Ok(())
    }
}

#[allow(dead_code)]
fn map_mcap(p: &str) -> anyhow::Result<Mmap> {
    let fd = fs::File::open(p).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

#[allow(dead_code)]
fn universal_schema() -> anyhow::Result<JSONSchema> {
    let schema = json!({});
    // the error is lifetime bound to the schema, so we can't return it
    if let Ok(compiled) = JSONSchema::options().compile(&schema) {
        Ok(compiled)
    } else {
        Err(anyhow!("Failed to compile universal schema"))
    }
}
