mod configuration;
mod upload;

use anyhow::{anyhow, Context};
use chrono::prelude::*;
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
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::{select, sync::mpsc::unbounded_channel};
use tracing::{dispatcher, error, info, warn, Dispatch};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, EnvFilter, Registry};
use upload::UploadConfig;

use crate::upload::upload_file;

const MQTT_MAX_PACKET_SIZE: usize = 268435455;
const MQTT_WILDCARD_TOPIC: &str = "#";
/// technically speaking we want to use AtLeastOnce
/// but honestly we don't care about message delivery here
const MQTT_LOGGER_QOS: QoS = QoS::AtMostOnce;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// mcap path
    #[arg(short, long)]
    out_dir: Option<String>,
    /// mqtt client id
    #[arg(long)]
    client_id: Option<String>,
    /// mqtt host
    #[arg(long)]
    broker_host: Option<String>,
    /// mqtt port
    #[arg(long)]
    broker_port: Option<u16>,
    /// config path
    #[arg(short, long)]
    config: Option<PathBuf>,
}

enum MqttUpdate {
    Message(Publish),
    StopLogger,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_tracing()?;

    let mut config = configuration::get_configuration(args.config)?;

    // move this somewhere else?
    if let Some(out_dir) = args.out_dir {
        config.logger.dir = out_dir;
    }
    if let Some(client_id) = args.client_id {
        config.mqtt.client_id = client_id;
    }
    if let Some(broker_host) = args.broker_host {
        config.mqtt.broker_host = broker_host;
    }
    if let Some(broker_port) = args.broker_port {
        config.mqtt.broker_port = broker_port;
    }

    // mqtt options
    let mut mqtt_options = MqttOptions::new(
        &config.mqtt.client_id,
        &config.mqtt.broker_host,
        config.mqtt.broker_port,
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    // outgoing is default
    mqtt_options.set_max_packet_size(MQTT_MAX_PACKET_SIZE, 10 * 1024);
    info!(?mqtt_options, "Starting MQTT server with options",);

    let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);

    client
        .subscribe(MQTT_WILDCARD_TOPIC, MQTT_LOGGER_QOS)
        .await
        .unwrap();
    let client_clone = client.clone();

    let (message_sender, mut message_receiver) = unbounded_channel();

    tokio::spawn(async move {
        loop {
            match event_loop.poll().await {
                Ok(notification) => match notification {
                    Event::Incoming(Incoming::Publish(publish)) => {
                        if let Err(e) = message_sender.send(MqttUpdate::Message(publish)) {
                            // a lot of these errors are probably because the receiver has been dropped
                            error!(error = %e, "Error sending message on mpsc channel, terminating");
                            break;
                        }
                    }
                    Event::Incoming(Incoming::ConnAck(_)) => {
                        info!("Connected to MQTT broker. Subscribing to all topics");
                        client
                            .subscribe(MQTT_WILDCARD_TOPIC, MQTT_LOGGER_QOS)
                            .await
                            .unwrap();
                    }
                    Event::Outgoing(Outgoing::Disconnect) => {
                        info!("Client disconnected. Shutting down");
                        // ignore error here
                        _ = message_sender.send(MqttUpdate::StopLogger);
                        break;
                    }

                    _ => (),
                },
                Err(e) => {
                    error!(error = %e, "Error processing event loop notifications");
                }
            }
        }
    });

    tokio::spawn(async move {
        // let ctrlc = tokio::signal::ctrl_c();
        let mut interrupt_stream =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
        let mut term_stream =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        select! {_ = interrupt_stream.recv() => (), _ = term_stream.recv() => ()};
        info!("Received interrupt, shutting down");
        client_clone.disconnect().await.unwrap();
    });

    let mut logger =
        RotatingMcapLogger::new(&config.logger.dir, config.azure_upload_config.clone())
            .context("Failed to create mcap writer")?;

    while let Some(message) = message_receiver.recv().await {
        match message {
            MqttUpdate::Message(publish) => {
                logger.write_message(&publish.topic, &publish.payload)?;
            }
            MqttUpdate::StopLogger => {
                info!("Received stop logger message");
                break;
            }
        }
    }

    logger.finish_existing()?;
    info!("Finalized Mcap logger");

    logger.finalize_all_uploads().await;

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
    uploader_config: Option<UploadConfig>,
    upload_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<'a> RotatingMcapLogger<'a> {
    pub fn new(
        directory_path: &str,
        uploader_config: Option<UploadConfig>,
    ) -> anyhow::Result<Self> {
        info!(directory_path, "Creating logging directory",);
        std::fs::create_dir_all(directory_path)?;
        let mut logger = Self {
            mcap_writer: None,
            last_rotation_time: Instant::now(),
            directory_path: directory_path.to_owned(),
            uploader_config,
            upload_tasks: vec![],
        };
        logger.start_new_file()?;
        Ok(logger)
    }

    pub fn write_message(&mut self, topic: &str, payload: &[u8]) -> anyhow::Result<()> {
        let time_since_rotation = self.last_rotation_time.elapsed();
        if time_since_rotation > ROTATION_INTERVAL {
            let seconds = time_since_rotation.as_secs();
            info!(
                seconds_since_rotation = seconds,
                "Rotation interval elapsed"
            );
            self.start_new_file()?;
        }

        // just in case there isn't one
        if self.mcap_writer.is_none() {
            warn!("mcap writer is none even though it shouldn't be");
            self.start_new_file()?;
        }

        self.mcap_writer
            .as_mut()
            .unwrap()
            .write_message(topic, payload)
    }

    fn start_new_file(&mut self) -> anyhow::Result<()> {
        self.finish_existing()?;

        let now_utc: DateTime<Utc> = Utc::now();
        let time = now_utc.to_rfc3339();

        let filename = format!("{}.mcap", time);
        let full_path = format!("{}/{}", self.directory_path, filename);

        info!(full_path, time, "starting new mcap file",);
        let mcap_writer = McapLogger::new(&full_path)?;
        self.mcap_writer = Some(mcap_writer);
        self.last_rotation_time = Instant::now();
        Ok(())
    }

    fn finish_existing(&mut self) -> anyhow::Result<()> {
        info!("Finishing existing mcap file");
        if let Some(mut mcap_writer) = self.mcap_writer.take() {
            mcap_writer.finish()?;

            let file_path = mcap_writer.file_path().to_owned();
            if let Some(uploader_config) = self.uploader_config.clone() {
                info!("Starting upload");
                let file_name = Path::new(&file_path)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let join_handle = tokio::spawn(async move {
                    if let Err(error) = upload_file(&file_path, &file_name, &uploader_config).await
                    {
                        error!("Failed upload {}", error);
                    } else {
                        info!("Upload complete");
                    }
                });

                // clean up old tasks
                self.upload_tasks.retain(|task| !task.is_finished());
                self.upload_tasks.push(join_handle);
            }
        }
        Ok(())
    }

    async fn finalize_all_uploads(&mut self) {
        info!("Finalizing all uploads");
        let upload_finished = futures::future::join_all(self.upload_tasks.drain(..)).await;
        for result in upload_finished {
            if let Err(error) = result {
                error!("Failed to finalize upload {}", error);
            }
        }
    }
}

struct McapLogger<'a> {
    file_path: String,
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
            file_path: file_path.to_owned(),
            mcap,
            channel_map: HashMap::new(),
            channel_message_counter: HashMap::new(),
        })
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
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
            info!(topic, "Adding json schema for topic");
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
            info!(topic, "Adding topic without schema");
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

pub fn setup_tracing() -> anyhow::Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .parse("")?;

    let subscriber = Registry::default()
        .with(filter)
        .with(tracing_logfmt::layer());
    dispatcher::set_global_default(Dispatch::new(subscriber))
        .context("Global logger has already been set!")?;
    Ok(())
}
