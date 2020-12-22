use crate::util;
use anyhow::Result;

pub async fn kill_all(host: &str) -> Result<()> {
    let host = util::resolve_host(host).await?;

    let cfg = chord::rpc::config(&host, None).await?;

    let mut fingers = util::aquire_nodes(host, &cfg).await?;

    fingers.dedup_by_key(|x| x.addr);

    for f in fingers {
        println!("quiting node at {}", f.addr);
        if let Err(_) = chord::rpc::quit(&f.addr, None).await {
            println!("Node already quit");
        };
    }
    Ok(())
}
