use anyhow::Result;
use humantime::parse_duration;
use std::time::Duration;
use structopt::StructOpt;

#[macro_use]
extern crate log;

mod keys;
mod kill_all;
mod lookup;
mod simulate;
mod start;
mod start_many;
mod util;

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
    /// Start nodes on the given addesses, using the first address as the bootstrap address.
    /// This command expects to have free ssh access to provided nodes as well as the rchord binary
    /// to be reachable from PATH.
    Simulate {
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
        start_port: Option<u16>,
        #[structopt(short = "r", long = "drop_interval", parse(try_from_str = parse_duration))]
        drop_interval: Duration,
        #[structopt(short = "l", long = "lookup_interval", parse(try_from_str = parse_duration))]
        lookup_interval: Duration,
        #[structopt(short = "d", long = "duration", parse(try_from_str = parse_duration))]
        test_duration: Duration,
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
    /// Stop all the nodes in the network
    KillAll { host: String },
    /// Start many nodes, possible running multiple nodes on the same cluster node"
    StartMany{
        /// The number of nodes to run in this network
        number_nodes: usize,
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
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<()> {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);
    let opt = Opt::from_args();
    debug!("opt: {:#?}", opt);
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
        Opt::Keys { host, key_amount} => {
            keys::keys(&host, key_amount).await?;
        }
        Opt::KillAll { host } => kill_all::kill_all(&host).await?,
        Opt::Simulate {
            nodes,
            num_bits,
            num_successors,
            num_virtual_nodes,
            update_interval,
            start_port,
            drop_interval,
            lookup_interval,
            test_duration,
        } => {
            simulate::simulate(
                nodes,
                num_bits.unwrap_or(16),
                num_successors.unwrap_or(4),
                num_virtual_nodes.unwrap_or(4),
                update_interval.unwrap_or(Duration::from_secs(2)),
                start_port.unwrap_or(8083u16),
                drop_interval,
                lookup_interval,
                test_duration,
            )
            .await?;
        }
        Opt::StartMany {
            number_nodes,
            nodes,
            num_bits,
            num_successors,
            num_virtual_nodes,
            update_interval,
            port,
        } => {
            start_many::start(
                nodes,
                num_bits.unwrap_or(16),
                num_successors.unwrap_or(4),
                num_virtual_nodes.unwrap_or(4),
                update_interval.unwrap_or(Duration::from_secs(2)),
                port.unwrap_or(8080),
                number_nodes,
            )
            .await?;
        }
    }
    Ok(())
}
