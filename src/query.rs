use crate::chord;
use anyhow::{anyhow, Context, Result};
use sha2::Digest;
use std::{collections::HashSet, net::SocketAddr};
use structopt::StructOpt;
use tokio::net;

#[derive(Debug, StructOpt)]
enum QueryKind {
    Key {
        value: String,
        #[structopt(short = "k", long = "key")]
        is_key: bool,
    },
    Nodes,
}

#[derive(Debug, StructOpt)]
pub struct Query {
    node: String,
    #[structopt(subcommand)]
    kind: QueryKind,
}

pub async fn query(query: &Query) -> Result<()> {
    let addr = net::lookup_host(&query.node)
        .await
        .context("looking up node address")?
        .next()
        .ok_or(anyhow!("no host found!"))?;

    let cfg = chord::rpc::config(&addr).await?;

    let start_key = chord::Key::new(&addr, 0, cfg.num_bits);

    let finger = chord::Finger {
        addr,
        id: start_key,
    };

    match query.kind {
        QueryKind::Key { ref value, is_key } => {
            query_key(finger, value, is_key, cfg.num_bits).await?;
        }
        QueryKind::Nodes => query_nodes(finger).await?,
    }

    Ok(())
}

pub async fn query_key(
    finger: chord::Finger,
    value: &str,
    is_key: bool,
    num_bits: u8,
) -> Result<()> {
    let key = if is_key {
        let num: u128 = value.parse().context("parsing value to a key number")?;
        chord::Key::from_number(num)
    } else {
        chord::Key::from_bytes(value.as_bytes(), num_bits)
    };

    info!(
        "connecting to {} to lookup \"{}\"={}",
        finger.addr, value, key
    );
    match chord::rpc::find_successor(&finger, key).await {
        Ok(Some(x)) => println!("found key in {}", x),
        Ok(None) => println!("failed to find key, network might be in unstable state"),
        Err(e) => return Err(e),
    }

    Ok(())
}

pub async fn query_nodes(mut finger: chord::Finger) -> Result<()> {
    let mut reached = HashSet::new();
    reached.insert(finger.id);
    let mut fingers = Vec::new();
    info!("querying all nodes, starting at {}", finger);
    loop {
        let successor = chord::rpc::successor(&finger).await?;
        finger = successor;
        fingers.push(finger.clone());
        if !reached.insert(finger.id) {
            break;
        }
    }
    println!("{}", serde_json::to_string_pretty(&fingers).unwrap());
    Ok(())
}
