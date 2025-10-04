use std::time::Duration;

use rand::{distr::Alphanumeric, prelude::*};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use tokio::task;

fn rnd_str_random() -> String {
    let rand_string: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    rand_string
}

fn rnd_u32_random() -> u32 {
    let mut rng = rand::rng();
    rng.random()
}

fn rnd_i64_random() -> i64 {
    let mut rng = rand::rng();
    rng.random()
}

fn generate_rand_struct() -> Payload {
    Payload {
        message: rnd_str_random(),
        id: rnd_u32_random(),
        complex: Nested {
            id: rnd_i64_random(),
            name: rnd_str_random(),
            items: [rnd_str_random(), rnd_str_random(), rnd_str_random()],
        },
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Nested {
    id: i64,
    name: String,
    items: [String; 3],
}

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    message: String,
    id: u32,
    complex: Nested,
}

#[tokio::main]
async fn main() {
    let result = do_main().await;
    if let Err(e) = result {
        println!("Error: {:?}", e);
        println!("Error: {:?}", e.root_cause());
    }
}

async fn do_main() -> anyhow::Result<()> {
    let mut mqttoptions = MqttOptions::new("rumqtt-sync2", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let topic_name = dotenvy::var("TOPIC_NAME")?;

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 101);

    task::spawn(async move {
        let mut counter = 0;
        client
            .subscribe(topic_name.as_str(), QoS::AtMostOnce)
            .await?;
        loop {
            client
                .publish(
                    topic_name.as_str(),
                    QoS::AtLeastOnce,
                    false,
                    serde_json::to_string(&generate_rand_struct()).unwrap(),
                )
                .await?;
            counter += 1;
            println!("Published {} messages", counter);
            if counter >= 100 {
                break;
            }
        }
        anyhow::Ok(())
    });
    println!("Publisher finished");

    while let Ok(notification) = eventloop.poll().await {
        println!("Notification: {:?}", notification);
    }

    Ok(())
}
