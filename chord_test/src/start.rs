use anyhow::{Result, bail, Context};
use std::time::Duration;
use duct::cmd;
use tokio::{net,time};


pub async fn start( 
        nodes: Vec<String>,
        num_bits: u8,
        num_successors: u32,
        num_virtual_nodes: u32,
        update_interval: Duration,
        port: u16
        ) -> Result<()>{

    let mut iter = nodes.iter();
    let main_node = if let Some(x) = iter.next(){
        x
    }else{
        bail!("No nodes provided, please provide at least a single node address")
    };
    let start_cmd = format!("tmux new-session -d -s chord \"rchord start {}:{} -b {} -s {} --nvirtuals {} -i {}\""
        ,&main_node
        ,port
        ,num_bits
        ,num_successors
        ,num_virtual_nodes
        ,humantime::format_duration(update_interval));


    cmd!("ssh",&main_node,start_cmd)
        .run()
        .with_context(||"failed to run bootstrap node start command")?;


    for n in iter{
        let command = format!("tmux new-session -d -s rchord connect {}:{} {}:{}",main_node,port,&n,port);
        cmd!("ssh",&n,command)
            .run()
            .with_context(||format!("failed to run node start command on '{}'",n))?;

    }


    info!("started nodes, giving them some time to get ready");
    time::sleep(Duration::from_secs(1)).await;

    info!("Pinging nodes");
    for n in nodes{
        info!("pinging {}",&n);
        let addr = net::lookup_host(format!("{}:{}",n,port)).await?.next().unwrap();
        chord::rpc::ping(&addr).await.with_context(|| format!("failed to connect to '{}' node might not have started", n))?;
    }

    info!("finished, all nodes running");
    Ok(())
}
