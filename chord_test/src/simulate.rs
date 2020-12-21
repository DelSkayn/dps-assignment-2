
use tokio::{sync::Mutex,time};
use std::{
    net::SocketAddr,
    time::Duration,
    sync::Arc,
    str,
    future::Future,
    time::Instant,
};
use rand::Rng;
use anyhow::{bail,Result,Context};
use duct::cmd;

fn force_anotate(f: impl Future<Output = Result<()>>) -> impl Future<Output = Result<()>>{
    f
}

pub async fn simulate(
        nodes: Vec<String>,
        num_bits: u8,
        num_successors: u32,
        num_virtual_nodes: u32,
        update_interval: Duration,
        start_port: u16,
        drop_interval: Duration,
        lookup_interval: Duration,
        test_duration: Duration) -> Result<()>{

    crate::start::start(nodes.clone(),num_bits,num_successors,num_virtual_nodes,update_interval,start_port).await?;

    let program = cmd!("which","rchord").stdout_capture().run().context("could not find rchord")?;
    if !program.status.success(){
        bail!("could not find rchord binary")
    }
    let program = str::from_utf8(&program.stdout).unwrap().trim().to_string();

    let node_address: Vec<(String,u16)> = nodes.into_iter().map(|x| (x,start_port)).collect();
    let node_address = Arc::new(Mutex::new(node_address));
    let node_address_clone = node_address.clone();
    tokio::spawn(force_anotate(async move{
        let node_address = node_address_clone;
        loop{
            time::sleep(drop_interval).await;
            let (mut pick,connect) = {
                let mut lock = node_address.lock().await;
                let pick = rand::thread_rng().gen_range(0..lock.len());
                let mut connect = pick; 
                while connect == pick{
                    connect = rand::thread_rng().gen_range(0..lock.len());
                }
                let res_pick = lock[pick].clone();
                let res_connect = lock[connect].clone();
                lock[pick].1 += 1;
                (res_pick,res_connect)
            };
            let addr = crate::resolve_host(&format!("{}:{}",pick.0,pick.1)).await?;
            info!("killing {:?}",pick);
            chord::rpc::quit(&addr,None).await?;
            pick.1 += 1;
            
            let command = format!("tmux new-session -d -s chord_{} \"{} connect {}:{} {}:{}\"",pick.1,program,pick.0,pick.1,connect.0,connect.1);
            dbg!(cmd!("ssh",&pick.0,command))
                .env("RUST_LOG","trace")
                .run()
                .with_context(||format!("failed to run node start command on '{}'",pick.0))?;

            chord::rpc::ping(&addr,None).await?;
        }
    }));

    let mut num_requests = 0;
    let mut failures = 0;
    let start = Instant::now();
    while start.elapsed() < test_duration{
        let max_key = (2 << num_bits) as u128;
        let key = rand::thread_rng().gen_range(0..max_key);
        let key = chord::Key::from_number(key);
        let node = {
            let lock = node_address.lock().await;
            let pick = rand::thread_rng().gen_range(0..lock.len());
            lock[pick].clone()
        };
        let addr = crate::resolve_host(&format!("{}:{}",node.0,node.1)).await?;
        let virtual_id = rand::thread_rng().gen_range(0..num_virtual_nodes);
        let finger = create_finger(addr,virtual_id,num_bits);
        if let Err(_) = chord::rpc::find_successor(&finger,key,None).await{
            failures += 1;
        }
        num_requests += 1;
        time::sleep(lookup_interval).await;
    }
    println!("did {} request of which {} failed",num_requests,failures);
    Ok(())
}

fn create_finger(addr: SocketAddr, virtual_id: u32,num_bits:u8) -> chord::Finger{
    let key = chord::Key::new(&addr,virtual_id,num_bits);
    chord::Finger{
        addr,
        id: key,
    }
}

