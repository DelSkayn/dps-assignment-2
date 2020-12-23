use anyhow::{anyhow, Result};
use chord::Finger;
use rand::Rng;
use std::{collections::HashSet, net::SocketAddr};

pub async fn resolve_host(host: &str) -> Result<SocketAddr> {
    tokio::net::lookup_host(host)
        .await?
        .next()
        .ok_or(anyhow!("failed to resolve initial node address"))
}

pub async fn aquire_nodes(host: SocketAddr, cfg: &chord::Config) -> Result<Vec<Finger>> {
    let mut finger = chord::Finger {
        addr: host.clone(),
        id: chord::Key::new(&host, 0, cfg.num_bits),
    };

    let mut reached = HashSet::new();
    reached.insert(finger.id);
    let mut fingers = Vec::new();
    info!("aquiring all nodes in the network, starting at {}", finger);
    loop {
        let successor = chord::rpc::successor(&finger, None).await?;
        info!("found {}", successor);
        finger = successor;
        fingers.push(finger.clone());
        if !reached.insert(finger.id) {
            break;
        }
    }
    info!("found a total of {} nodes",fingers.len());
    Ok(fingers)
}

pub fn random_key(num_bits: u8) -> chord::Key {
    let max_key = 2 << (num_bits as u128);
    let key = rand::thread_rng().gen_range(0..max_key);
    chord::Key::from_number(key)
}

pub fn create_finger(addr: SocketAddr, virtual_id: u32, num_bits: u8) -> chord::Finger {
    let key = chord::Key::new(&addr, virtual_id, num_bits);
    chord::Finger { addr, id: key }
}
