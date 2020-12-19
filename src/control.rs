use super::chord;

use anyhow::{anyhow, Context, Result};
use tokio::net;

pub async fn quit(host: &str) -> Result<()> {
    let addr = net::lookup_host(&host)
        .await
        .context("looking up node address")?
        .next()
        .ok_or(anyhow!("no host found!"))?;

    chord::rpc::quit(&addr).await?;
    Ok(())
}

pub async fn add(host: &str, value: &str, is_key: bool) -> Result<()> {
    let addr = net::lookup_host(&host)
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

    let key = if is_key {
        let num: u128 = value.parse().context("parsing value to a key number")?;
        chord::Key::from_number(num, cfg.num_bits)
    } else {
        chord::Key::from_bytes(value.as_bytes(), cfg.num_bits)
    };
    let successor = match chord::rpc::find_successor(&finger, key).await {
        Ok(Some(x)) => {
            println!("found key in {}", x);
            x
        }
        Ok(None) => bail!("failed to find key, network might be in unstable state"),
        Err(e) => return Err(e),
    };

    chord::rpc::add_key(&successor, key).await
}

pub async fn get(host: &str, value: &str, is_key: bool) -> Result<()> {
    let addr = net::lookup_host(&host)
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

    let key = if is_key {
        let num: u128 = value.parse().context("parsing value to a key number")?;
        chord::Key::from_number(num, cfg.num_bits)
    } else {
        chord::Key::from_bytes(value.as_bytes(), cfg.num_bits)
    };
    let successor = match chord::rpc::find_successor(&finger, key).await {
        Ok(Some(x)) => x,
        Ok(None) => bail!("failed to find key, network might be in unstable state"),
        Err(e) => return Err(e),
    };

    if chord::rpc::contains_key(&successor, key).await? {
        println!("found: {}", successor);
    } else {
        println!("key not found");
    }
    Ok(())
}
