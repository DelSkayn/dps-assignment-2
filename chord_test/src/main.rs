use anyhow::Result;
use chord::Finger;
use humantime::parse_duration;
use std::{collections::HashSet, net::SocketAddr, time::Duration};
use structopt::StructOpt;

#[macro_use]
extern crate log;

mod keys;
mod lookup;
mod start;

async fn aquire_nodes(host: SocketAddr, cfg: &chord::Config) -> Result<Vec<Finger>> {
    let mut finger = chord::Finger {
        addr: host.clone(),
        id: chord::Key::new(&host, 0, cfg.num_bits),
    };

    let mut reached = HashSet::new();
    reached.insert(finger.id);
    let mut fingers = Vec::new();
    info!("aquiring all nodes in the network, starting at {}", finger);
    loop {
        let successor = chord::rpc::successor(&finger, None).await?;
        info!("found {}", successor);
        finger = successor;
        fingers.push(finger.clone());
        if !reached.insert(finger.id) {
            break;
        }
    }
    Ok(fingers)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "rchord-test")]
enum Opt {
    /// Start nodes on the given addesses, using the first address as the bootstrap address.
    /// This command expects to have free ssh access to provided nodes as well as the rchord binary
    /// to be reachable from PATH.
    Start {
        /// Address for the network nodes to run on.
        nodes: Vec<String>,
        /// The number of bits in the key (default 16)
        #[structopt(short = "b", long = "bits")]
        num_bits: Option<u8>,
        /// The number of successors to keep track of (default 4)
        #[structopt(short = "s", long = "successors")]
        num_successors: Option<u32>,
        /// The number of virtual nodes to run on a single node (default 4)
        #[structopt(long = "nvirtuals")]
        num_virtual_nodes: Option<u32>,
        /// The time interval between the various update ticks performed by chord (default 2s)
        #[structopt(short = "i", long = "interval", parse(try_from_str = parse_duration))]
        update_interval: Option<Duration>,
        /// port to run the chord protocol on (default 8080).
        #[structopt(short = "p", long = "port")]
        port: Option<u16>,
    },
    /// Run a test on lookup performance
    Lookup {
        host: String,
        repeat: u64,
        request_per_second: usize,
        output: Option<String>,
    },
    /// Assign random keys to nodes and query to amount of keys in each node
    Keys { host: String, key_amount: usize },
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);
    let opt = Opt::from_args();
    match opt {
        Opt::Start {
            nodes,
            num_bits,
            num_successors,
            num_virtual_nodes,
            update_interval,
            port,
        } => {
            start::start(
                nodes,
                num_bits.unwrap_or(16),
                num_successors.unwrap_or(4),
                num_virtual_nodes.unwrap_or(4),
                update_interval.unwrap_or(Duration::from_secs(2)),
                port.unwrap_or(8080),
            )
            .await?;
        }
        Opt::Lookup {
            host,
            repeat,
            request_per_second,
            output,
        } => {
            lookup::lookup(&host, repeat, request_per_second, output).await?;
        }
        Opt::Keys { host, key_amount } => {
            keys::keys(&host, key_amount).await?;
        }
    }
    Ok(())
}
