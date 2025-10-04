pub mod db;
pub mod manager;
pub mod mapper;
pub mod utils;
use bytes::Bytes;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Deserialize;
use serde_humantime;
use std::collections::HashMap;
use std::time::Duration;

use tokio::spawn;
use tokio::{self, sync::mpsc};

use crate::{
    db::{DBDriver, MQTable, PostgresDriver},
    manager::Manager,
    mapper::json_to_data_row,
};

use chrono::prelude::*;

pub struct MessagePayload {
    pub topic: String,
    pub payload: Bytes,
    pub timestamp: DateTime<Utc>,
}

impl From<(String, Bytes, DateTime<Utc>)> for MessagePayload {
    fn from(value: (String, Bytes, DateTime<Utc>)) -> Self {
        Self {
            topic: value.0,
            payload: value.1,
            timestamp: value.2,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Deserialize)]
pub struct Config {
    mqtt_id: String,
    mqtt_host: String,
    #[serde(flatten)]
    inner: DefaultConfig,
}

impl Config {
    pub fn to_mqtt_options(&self) -> MqttOptions {
        MqttOptions::new(
            self.mqtt_id.clone(),
            self.mqtt_host.clone(),
            self.inner.mqtt_port,
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Deserialize)]
#[serde(default)]
pub struct DefaultConfig {
    batch_count: usize,
    mqtt_eventloop_capacity: usize,
    mqtt_port: u16,
    #[serde(with = "serde_humantime")]
    mqtt_keepalive: Duration,
}

impl Default for DefaultConfig {
    fn default() -> Self {
        Self {
            batch_count: 100,
            mqtt_eventloop_capacity: 100,
            mqtt_port: 1883,
            mqtt_keepalive: Duration::from_secs(5),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = do_main().await;

    if let Err(e) = result {
        println!("Error: {}", e);
    }
}

async fn do_main() -> anyhow::Result<()> {
    dotenvy::dotenv_override()?;
    let configs: Config = envy::from_env()?;

    println!("Running with configs \n{configs:#?}");

    let mqttoptions = configs.to_mqtt_options();

    let topic_name = dotenvy::var("TOPIC_NAME")?;

    let (client, mut eventloop) =
        AsyncClient::new(mqttoptions, configs.inner.mqtt_eventloop_capacity);
    client
        .subscribe(topic_name.as_str(), QoS::AtMostOnce)
        .await?;

    let mut manager =
        Manager::new(PostgresDriver::connect(dotenvy::var("DATABASE_URL")?.as_str()).await?);

    manager
        .initialize(&MQTable::from_topic(topic_name.as_str()))
        .await?;

    println!("Manager initialized");

    let (tx, rx) = mpsc::unbounded_channel::<MessagePayload>();

    // let handle =
    spawn(async move {
        let out = {
            let mut rx = rx;

            let mut buffer = vec![];

            let mut map: HashMap<MQTable, Vec<_>> = HashMap::new();

            loop {
                let msgs = rx.recv_many(&mut buffer, configs.inner.batch_count).await;
                if msgs == 0 {
                    // channel is closed go home
                    break;
                }

                for MessagePayload {
                    topic,
                    payload,
                    timestamp,
                } in buffer.drain(..)
                {
                    let table = MQTable::from_topic(&topic);
                    println!(
                        "Received on topic {} - {} at {}: {:?}",
                        topic, table.name, timestamp, payload
                    );
                    let obj =
                        json_to_data_row(String::from_utf8(payload.to_vec())?.as_str(), timestamp)?;

                    map.entry(table).or_default().push(obj);
                }

                for (key, value) in map.drain() {
                    let err = manager.insert_many(&key, &value).await;
                    if err.is_err() {
                        println!("{:?}", err);
                    }
                    err?;
                }

                println!("Inserted into DB");
            }

            anyhow::Ok(())
        };
        if out.is_err() {
            println!("{:?}", out);
        }
        out
    });

    loop {
        let notification = eventloop.poll().await?;
        println!("Notification: {:?}", notification);
        match notification {
            rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) => {
                tx.send((p.topic, p.payload, Utc::now()).into())?;
            }
            _ => {}
        }
    }

    // move this to RAII drop impl
    // drop(tx);

    // return handle.await?;
}
