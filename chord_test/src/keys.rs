use super::aquire_nodes;
use anyhow::{anyhow, Context, Result};
use rand::Rng;
use tokio::net;

pub async fn keys(host: &str, key_amount: usize) -> Result<()> {
    let host = net::lookup_host(host)
        .await?
        .next()
        .ok_or(anyhow!("failed to resolve initial node address"))?;

    let cfg = chord::rpc::config(&host, None)
        .await
        .context("failed to retrieve network config")?;

    let fingers = aquire_nodes(host, &cfg).await?;

    let mut handles = Vec::new();
    for _ in 0..key_amount {
        let finger = rand::thread_rng().gen_range(0..fingers.len());
        let finger = fingers[finger].clone();
        let max_key = (2 << cfg.num_bits) as u128;
        let key = rand::thread_rng().gen_range(0..max_key);
        let key = chord::Key::from_number(key);
        handles.push(tokio::spawn(async move {
            chord::rpc::add_key(&finger, key, None).await
        }));
    }

    for h in handles {
        h.await??;
    }

    let mut num_keys = Vec::new();
    for f in fingers {
        let info = chord::rpc::info(&f, None).await?;
        num_keys.push((f, info.num_keys));
    }

    println!("node : keys");
    for f in num_keys {
        println!("{}:{}", f.0, f.1)
    }

    Ok(())
}
