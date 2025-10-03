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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let topic_name = "hello/rumqtt";

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe(topic_name, QoS::AtMostOnce).await.unwrap();

    let (tx, rx) = mpsc::unbounded_channel::<(String, Bytes)>();

    spawn(async move {
        let mut manager =
            Manager::new(PostgresDriver::connect(dotenvy::var("DATABASE_URL")?.as_str()).await?);

        let mut rx = rx;
        while let Some((topic, payload)) = rx.recv().await {
            let table = MQTable::from_topic(&topic);
            println!(
                "Received on topic {} - {}: {:?}",
                topic, table.name, payload
            );

            let obj = json_to_data_row(String::from_utf8(payload.to_vec())?.as_str())?;
            manager.insert(&table, obj).await?;
        }
        anyhow::Ok(())
    });

    while let Ok(notification) = eventloop.poll().await {
        // println!("Received = {:?}", notification);
        match notification {
            rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) => {
                println!("Payload = {:?}", p.payload);
                tx.send((p.topic, p.payload)).unwrap();
            }
            _ => { /*println!("Other = {:?}", notification);*/ }
        }
    }

    Ok(())
}
