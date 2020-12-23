use super::util;
use anyhow::{Context, Result};
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};

pub async fn keys(host: &str, key_amount: usize) -> Result<()> {
    let host = util::resolve_host(host).await?;
    let cfg = chord::rpc::config(&host, None)
        .await
        .context("failed to retrieve network config")?;

    let fingers = util::aquire_nodes(host, &cfg).await?;

    let mut node_keys = HashMap::<(SocketAddr,u16),usize>::new();
    for f in fingers.iter(){
        node_keys.entry((f.addr,f.addr.port())).or_insert(0);
    }
    let mut handles = VecDeque::new();
    eprintln!();
    for i in 0..key_amount {
        eprintln!("\rloading key {}",i);
        let finger = rand::thread_rng().gen_range(0..fingers.len());
        let finger = fingers[finger].clone();
        let key = util::random_key(cfg.num_bits);
        handles.push_back(tokio::spawn(async move {
            let successor = chord::rpc::find_successor(&finger,key,None).await?;
            Result::<chord::Finger>::Ok(successor.unwrap())
        }));
        while handles.len() > 8{
            let succ = handles.pop_front().unwrap().await??;
            *node_keys.entry((succ.addr,succ.addr.port())).or_insert(0) += 1;
        }
    }

    for h in handles {
        let succ = h.await??;
        *node_keys.entry((succ.addr,succ.addr.port())).or_insert(0) += 1;
    }

    println!("node,number_of_keys");
    for (node,number) in node_keys {
        println!("{},{}",node.0,number);
    }

    Ok(())
}
