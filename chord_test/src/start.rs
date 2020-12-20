use anyhow::{Result, bail, Context};
use std::time::Duration;
use tokio::process::Command;


pub async fn start( 
        nodes: Vec<String>,
        num_bits: u8,
        num_successors: u32,
        num_virtual_nodes: u32,
        update_interval: Duration) -> Result<()>{

    let mut iter = nodes.into_iter();
    let main_node = if let Some(x) = iter.next(){
        x
    }else{
        bail!("No nodes provided, please provide at least a single node address")
    };
    Command::new("ssh")
        .arg(&main_node)
        .arg(format!("rchord start {} -b {} -s {} --nvirtuals {} -i {}",main_node,num_bits, num_successors, num_virtual_nodes, humantime::format_duration(update_interval)))
        .output()
        .await
        .with_context(|| format!("Failed to start bootstrap node on {}",main_node))?;

    let futures = iter.map(|x|
        (Command::new("ssh")
                .arg(&x)
                .arg(format!("rchord connect {} {}",main_node,x))
                .output(),
                x)
    );
    for f in futures.into_iter(){
        let node = f.1;
        f.0.await
        .with_context(|| format!("Failed to start normal node on {}",node))?;
    }
    info!("finished, all nodes running");
    Ok(())
}
