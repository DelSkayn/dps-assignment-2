use anyhow::{Result, bail, Context};
use std::{str,time::Duration};
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
    let program = cmd!("which","rchord").stdout_capture().run().context("could not find rchord")?;
    if !program.status.success(){
        bail!("could not find rchord binary")
    }
    let program = str::from_utf8(&program.stdout).unwrap().trim().to_string();

    let start_cmd = format!("tmux new-session -d -s chord \"{} start {}:{} -b {} -s {} --nvirtuals {} -i {}\""
        ,program
        ,&main_node
        ,port
        ,num_bits
        ,num_successors
        ,num_virtual_nodes
        ,humantime::format_duration(update_interval));


    dbg!(cmd!("ssh",&main_node,start_cmd))
        .env("RUST_LOG","trace")
        .run()
        .with_context(||"failed to run bootstrap node start command")?;


    for n in iter{
        let command = format!("tmux new-session -d -s chord \"{} connect {}:{} {}:{}\"",program,&n,port,main_node,port);
        dbg!(cmd!("ssh",&n,command))
            .env("RUST_LOG","trace")
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
