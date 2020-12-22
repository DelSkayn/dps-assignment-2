use crate::util;
use anyhow::{bail, Context, Result};
use duct::cmd;
use rand::{seq::SliceRandom, Rng};
use std::{future::Future, str, sync::Arc, time::Duration, time::Instant};
use tokio::{process, sync::Mutex, time};

fn force_anotate(f: impl Future<Output = Result<()>>) -> impl Future<Output = Result<()>> {
    f
}

pub async fn simulate(
    nodes: Vec<String>,
    num_bits: u8,
    num_successors: u32,
    num_virtual_nodes: u32,
    update_interval: Duration,
    start_port: u16,
    drop_interval: Duration,
    lookup_interval: Duration,
    test_duration: Duration,
) -> Result<()> {
    crate::start::start(
        nodes.clone(),
        num_bits,
        num_successors,
        num_virtual_nodes,
        update_interval,
        start_port,
    )
    .await?;

    let program = cmd!("which", "rchord")
        .stdout_capture()
        .run()
        .context("could not find rchord")?;
    if !program.status.success() {
        bail!("could not find rchord binary")
    }
    let program = str::from_utf8(&program.stdout).unwrap().trim().to_string();

    let node_address: Vec<(String, u16)> = nodes.into_iter().map(|x| (x, start_port)).collect();
    let node_address = Arc::new(Mutex::new(node_address));
    let node_address_clone = node_address.clone();
    tokio::spawn(force_anotate(async move {
        let node_address = node_address_clone;
        let mut interval = time::interval(drop_interval);
        let len = node_address.lock().await.len();
        let mut picks: Vec<_> = (0..len).collect();
        picks.shuffle(&mut rand::thread_rng());

        loop {
            interval.tick().await;
            let (mut pick, connect) = {
                let pick = if let Some(x) = picks.pop() {
                    x
                } else {
                    let mut picks: Vec<_> = (0..len).collect();
                    picks.shuffle(&mut rand::thread_rng());
                    picks.pop().unwrap()
                };
                let mut connect = pick;
                while connect == pick {
                    connect = rand::thread_rng().gen_range(0..len);
                }
                let mut lock = node_address.lock().await;
                let res_pick = lock[pick].clone();
                let res_connect = lock[connect].clone();
                lock[pick].1 += 1;
                (res_pick, res_connect)
            };
            let addr = util::resolve_host(&format!("{}:{}", pick.0, pick.1)).await?;
            info!("killing {:?}", pick);
            chord::rpc::quit(&addr, None).await?;
            pick.1 += 1;

            let command = format!(
                "nohup {} connect {}:{} {}:{} > /dev/null 2>&1 < /dev/null &",
                program, pick.0, pick.1, connect.0, connect.1
            );

            dbg!(&command);

            process::Command::new("ssh")
                .env("RUST_LOG", "trace")
                .arg("-n")
                .arg(&pick.0)
                .arg(command)
                .status()
                .await?;
        }
    }));

    let mut num_requests = 0;
    let mut failures = 0;
    let start = Instant::now();
    while start.elapsed() < test_duration {
        let key = util::random_key(num_bits);
        let node = {
            let lock = node_address.lock().await;
            let pick = rand::thread_rng().gen_range(0..lock.len());
            lock[pick].clone()
        };
        let addr = util::resolve_host(&format!("{}:{}", node.0, node.1)).await?;
        let virtual_id = rand::thread_rng().gen_range(0..num_virtual_nodes);
        let finger = util::create_finger(addr, virtual_id, num_bits);
        trace!("looking for key {} in {}", key, finger);
        if let Err(_) = chord::rpc::find_successor(&finger, key, None).await {
            failures += 1;
        }
        num_requests += 1;
        time::sleep(lookup_interval).await;
    }
    println!("did {} request of which {} failed", num_requests, failures);
    Ok(())
}
