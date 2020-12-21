use anyhow::{Context, Result};
use rand::Rng;
use std::time::{Duration, Instant};
use tokio::{fs::File, io::AsyncWriteExt, net, time};

use crate::aquire_nodes;

#[derive(Debug)]
struct RequestInfo {
    time: Duration,
    lookups: usize,
}

pub async fn lookup(
    start: &str,
    repeat: u64,
    request_per_second: usize,
    output: Option<String>,
) -> Result<()> {
    let addr = net::lookup_host(start)
        .await
        .context("failed to lookup host")?
        .next()
        .context("could not resolve host name")?;

    let cfg = chord::rpc::config(&addr, None)
        .await
        .context("failed to retrieve network configuration from start node")?;

    let fingers = aquire_nodes(addr, &cfg).await?;

    let mut handle = Vec::new();
    for i in 0..(repeat as usize) {
        if i % request_per_second == request_per_second - 1 {
            info!("did {} requests", i);
            time::sleep(Duration::from_secs(1)).await;
        }

        let max_key = (2 << cfg.num_bits) as u128;
        let key = rand::thread_rng().gen_range(0u128..max_key);
        let key = chord::Key::from_number(key);
        let finger = rand::thread_rng().gen_range(0..fingers.len());
        let finger = fingers[finger].clone();
        handle.push(tokio::spawn(request(finger, key)));
    }

    let mut time = Duration::from_secs(0);
    let mut lookups = Vec::<usize>::new();
    let mut times = 0usize;
    let mut data = Vec::new();
    for h in handle {
        let x = h.await??;
        times += 1;
        time += x.time;
        while lookups.len() < x.lookups + 1 {
            lookups.push(0);
        }
        lookups[x.lookups] += 1;
        data.push(x);
    }

    println!(
        "average time: {}",
        humantime::format_duration(time.div_f64(times as f64))
    );
    println!("lookups: num requests: times (total times {}): ", times);
    for (i, l) in lookups.iter().enumerate() {
        println!("{}: {}", i, l);
    }

    if let Some(x) = output {
        println!("writing data to '{}'", x);
        let mut file = File::create(x).await?;
        for (i, r) in data.iter().enumerate() {
            let buffer = format!("{},{},{}\n", i, r.lookups, r.time.as_nanos());
            file.write_all(buffer.as_bytes()).await?;
        }
    }

    Ok(())
}

async fn request(finger: chord::Finger, key: chord::Key) -> Result<RequestInfo> {
    let time = Instant::now();
    let mut lookups = 1;
    let mut closest = finger.clone();
    let mut succ = chord::rpc::successor(&closest, None).await?;
    while !key.within(&closest.id.to(succ.id)) {
        closest = chord::rpc::find_closest_predecessor(&closest, key, None).await?;
        lookups += 1;
        let tmp = chord::rpc::successor(&closest, None).await?;
        lookups += 1;
        if tmp.id == succ.id {
            break;
        }
        succ = tmp
    }
    Ok(RequestInfo {
        time: Instant::now().duration_since(time),
        lookups,
    })
}
