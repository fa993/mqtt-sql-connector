use std::time::{Duration, Instant};

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

fn generate_rand_struct(rate: String, group: String) -> Payload {
    Payload {
        message: rnd_str_random(),
        id: rnd_u32_random(),
        complex: Nested {
            id: rnd_i64_random(),
            name: rnd_str_random(),
            items: [rnd_str_random(), rnd_str_random(), rnd_str_random()],
        },
        rate_at: rate,
        iter_count: group,
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
    rate_at: String,
    iter_count: String,
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
        client
            .subscribe(topic_name.as_str(), QoS::AtMostOnce)
            .await?;

        let pusher = Pusher::new(client, topic_name);

        // iterate through several rates exponentially
        // 10 per second for a minute,
        // 100 per second for a minute
        // 1000 per second for a minute
        // 10000 per second for a minute

        pusher.exec_at_rate(10).await?;
        pusher.exec_at_rate(100).await?;
        pusher.exec_at_rate(1000).await?;
        pusher.exec_at_rate(10000).await?;

        println!("Publisher finished");

        anyhow::Ok(())
    });

    while let Ok(_) = eventloop.poll().await {}

    Ok(())
}

pub struct Pusher {
    client: AsyncClient,
    topic: String,
}

impl Pusher {
    pub fn new(client: AsyncClient, topic: String) -> Self {
        Self { client, topic }
    }

    pub async fn push(&self, rate: String, count: u32) -> anyhow::Result<()> {
        println!("Publishing message {} {}", count, rate);
        self.client
            .publish(
                self.topic.as_str(),
                QoS::AtLeastOnce,
                false,
                serde_json::to_string(&generate_rand_struct(rate, format!("group-{}", count)))?,
            )
            .await?;
        Ok(())
    }

    pub async fn exec_at_rate(&self, rate: u64) -> anyhow::Result<()> {
        let nanoseconds = (1000_000_000.0 / rate as f64).floor() as u64;
        let interval = Duration::from_nanos(nanoseconds);
        println!("Duration {:?}", interval);
        let start_time = Instant::now();
        let mut counter: u32 = 0;
        let mut interval_timer = tokio::time::interval(interval);
        while start_time.elapsed() < Duration::from_secs(60) {
            interval_timer.tick().await;
            let ts = Instant::now();
            self.push(format!("{} msg/sec", rate), counter).await?;
            println!(
                "Pushed message {} at rate {} in {:?}",
                counter,
                rate,
                Instant::now().duration_since(ts)
            );
            counter += 1;
        }
        anyhow::Ok(())
    }
}
