use anyhow::{Context,Result};
use tokio::net;

pub async fn lookup(start: &str, repeat: u64, request_per_second: u64) -> Result<()>{
    let addr = net::lookup_host(start).await.context("failed to lookup host")?;

    let cfg = chord::rpc::config(&addr)
        .await
        .context("failed to retrieve network configuration from start node")?;

    let finger = Finger{
        addr: addr.clone(),
        id: chord::Key::new(&addr,0,cfg.num_bits),
    };

    let mut reached = HashSet::new();
    reached.insert(finger.id);
    let mut fingers = Vec::new();
    info!("aquiring all nodes in the network, starting at {}", finger);
    loop {
        let successor = chord::rpc::successor(&finger).await?;
        info!("found {}",successor);
        finger = successor;
        fingers.push(finger.clone());
        if !reached.insert(finger.id) {
            break;
        }
    }

}

pub async fn request(finger: &Finger, channel: ) -> Result<()>{
        let succ = self.get_successor().await;
        if key.within(&self.this.id.to(succ.id)) {
            return Some(succ);
        }
        let mut closest = self.find_closest_predecessor(key).await;
        let mut succ = match rpc::successor(&closest).await {
            Ok(x) => x,
            Err(_) => {
                self.invalidate_node(succ.id).await;
                return None;
            }
        };
        while !key.within(&closest.id.to(succ.id)) {
            closest = match rpc::find_closest_predecessor(&closest, key).await {
                Ok(x) => x,
                Err(_) => {
                    self.invalidate_node(closest.id).await;
                    return None;
                }
            };
            let tmp = match rpc::successor(&closest).await {
                Ok(x) => x,
                Err(_) => {
                    self.invalidate_node(closest.id).await;
                    return None;
                }
            };
            if tmp.id == succ.id {
                return Some(succ);
            }
            succ = tmp
        }
        return Some(succ);
}

