use anyhow::{bail, Context, Result};
use duct::cmd;
use std::{str, time::Duration};
use tokio::{net, time};

pub async fn start(
    nodes: Vec<String>,
    num_bits: u8,
    num_successors: u32,
    num_virtual_nodes: u32,
    update_interval: Duration,
    port: u16,
    num_nodes: usize,
) -> Result<()> {
    assert!(num_nodes > 1);
    let mut iter = nodes.iter();
    let main_node = if let Some(x) = iter.next() {
        x
    } else {
        bail!("No nodes provided, please provide at least a single node address")
    };
    let main_port = port;
    let program = cmd!("which", "rchord")
        .stdout_capture()
        .run()
        .context("could not find rchord")?;
    if !program.status.success() {
        bail!("could not find rchord binary")
    }
    let program = str::from_utf8(&program.stdout).unwrap().trim().to_string();

    let start_cmd = format!(
        "nohup {} start {}:{} -b {} -s {} --nvirtuals {} -i {} > /dev/null 2>&1 < /dev/null &",
        program,
        &main_node,
        port,
        num_bits,
        num_successors,
        num_virtual_nodes,
        humantime::format_duration(update_interval)
    );

    info!("starting bootstrap node: {}", main_node);

    cmd!("ssh", "-n", &main_node, start_cmd)
        .run()
        .with_context(|| "failed to run bootstrap node start command")?;

    let mut ports = vec![port; nodes.len()];
    ports[0] += 1;

    let mut total_nodes = Vec::new();
    for i in 0..(num_nodes-1){
        let idx = (i + 1) % ports.len();
        let port = ports[idx];
        let addr = nodes[idx].clone();
        ports[idx] += 1;
        total_nodes.push((addr.clone(),port));
        let command = format!(
            "nohup {} connect {}:{} {}:{} > /dev/null 2>&1 < /dev/null &",
            program, &addr, port, main_node, main_port
        );
        info!("starting node: {}:{}", addr,port);
        cmd!("ssh", "-n", &addr, command)
            .run()
            .with_context(|| format!("failed to run node start command on '{}'", addr))?;
    }

    info!("started nodes, giving them some time to get ready");
    time::sleep(Duration::from_secs(1)).await;

    info!("Pinging nodes");
    for (addr,port) in total_nodes{
        info!("pinging {}:{}", &addr,port);
        let addr = net::lookup_host(format!("{}:{}", addr, port))
            .await?
            .next()
            .unwrap();
        chord::rpc::ping(&addr, None)
            .await
            .with_context(|| format!("failed to connect to '{}:{}' node might not have started", addr,port))?;
    }

    info!("finished, all nodes running");
    Ok(())
}
