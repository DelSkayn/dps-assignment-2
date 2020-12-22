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
) -> Result<()> {
    let mut iter = nodes.iter();
    let main_node = if let Some(x) = iter.next() {
        x
    } else {
        bail!("No nodes provided, please provide at least a single node address")
    };
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

    for n in iter {
        let command = format!(
            "nohup {} connect {}:{} {}:{} > /dev/null 2>&1 < /dev/null &",
            program, &n, port, main_node, port
        );
        info!("starting node: {}", n);
        cmd!("ssh", "-n", &n, command)
            .run()
            .with_context(|| format!("failed to run node start command on '{}'", n))?;
    }

    info!("started nodes, giving them some time to get ready");
    time::sleep(Duration::from_secs(1)).await;

    info!("Pinging nodes");
    for n in nodes {
        info!("pinging {}", &n);
        let addr = net::lookup_host(format!("{}:{}", n, port))
            .await?
            .next()
            .unwrap();
        chord::rpc::ping(&addr, None)
            .await
            .with_context(|| format!("failed to connect to '{}' node might not have started", n))?;
    }

    info!("finished, all nodes running");
    Ok(())
}
