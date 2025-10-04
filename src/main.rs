pub mod db;
pub mod manager;
pub mod mapper;
pub mod utils;
use bytes::Bytes;
use rumqttc::{AsyncClient, MqttOptions, QoS};
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

#[tokio::main]
async fn main() {
    let result = do_main().await;

    if let Err(e) = result {
        println!("Error: {}", e);
    }
}

async fn do_main() -> anyhow::Result<()> {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let topic_name = dotenvy::var("TOPIC_NAME")?;

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
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
        let mut rx = rx;

        while let Some(MessagePayload {
            topic,
            payload,
            timestamp,
        }) = rx.recv().await
        {
            let table = MQTable::from_topic(&topic);
            println!(
                "Received on topic {} - {} at {}: {:?}",
                topic, table.name, timestamp, payload
            );

            let obj = json_to_data_row(String::from_utf8(payload.to_vec())?.as_str(), timestamp)?;
            manager.insert(&table, obj).await?;
            println!("Inserted into DB");
        }
        anyhow::Ok(())
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
