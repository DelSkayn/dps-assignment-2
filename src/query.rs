use crate::chord;
use anyhow::{anyhow, Context, Result};
use sha2::Digest;
use tokio::net;

pub async fn query(
    node: String,
    value: String,
    virtual_node: u32,
    is_key: bool,
    num_bits: u8,
) -> Result<()> {
    let addr = net::lookup_host(node)
        .await
        .context("looking up node address")?
        .next()
        .ok_or(anyhow!("no host found!"))?;

    let query_key = chord::Key::new(&addr, virtual_node, num_bits);

    let key = if is_key {
        let num: u128 = value.parse().context("parsing value to a key number")?;
        chord::Key::from_number(num, num_bits)
    } else {
        chord::Key::from_bytes(value.as_bytes(), num_bits)
    };

    let finger = chord::Finger {
        addr,
        id: query_key,
    };

    info!("connecting to {} to lookup \"{}\"={}", addr, value, key);
    match chord::rpc::find_successor(&finger, key).await {
        Ok(Some(x)) => println!("found key in {}", x),
        Ok(None) => println!("failed to find key, network might be in unstable state"),
        Err(e) => return Err(e),
    }

    Ok(())
}
