#![allow(dead_code)]
#![allow(unused_imports)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;

use anyhow::Result;
use humantime::parse_duration;
use std::time::Duration;
use structopt::StructOpt;

mod control;
mod query;

pub use chord::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "rchord")]
enum Opt {
    /// Query the network for information.
    Query(query::Query),
    /// Run a chord node as the start of a network.
    Start {
        /// The address to host the node on
        host: String,
        /// The number of bits to use in the keys of this network. (default 16)
        #[structopt(short = "b", long = "bits")]
        num_bits: Option<u8>,
        /// The number of successors each node should maintain (default 4)
        #[structopt(short = "s", long = "successors")]
        num_successors: Option<u32>,
        /// The number of virtual nodes per full node (default 4)
        #[structopt(long = "nvirtuals")]
        num_virtual_nodes: Option<u32>,
        /// The interval between stabilization requests done per node.
        #[structopt(short = "i", long = "interval", parse(try_from_str = parse_duration))]
        update_interval: Option<Duration>,
    },
    /// Run a new chord node connecting to a already running network.
    Connect { host: String, bootstrap: String },
    /// Stop a running node at address host
    Quit { host: String },
    /// Add a key into the network.
    Add {
        /// The address of the node to use for the lookup
        host: String,
        /// The value to add
        value: String,
        /// Is the value a key or should it be hashed
        #[structopt(short = "k", long = "key")]
        is_key: bool,
    },
    /// Get a key from the network.
    Get {
        /// The address of the node to use for the lookup
        host: String,
        /// The value to add
        value: String,
        /// Is the value a key or should it be hashed
        #[structopt(short = "k", long = "key")]
        is_key: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    match opt {
        Opt::Start {
            host,
            num_bits,
            num_successors,
            update_interval,
            num_virtual_nodes,
        } => {
            let cfg = chord::Config {
                num_bits: num_bits.unwrap_or(16),
                num_successors: num_successors.unwrap_or(3),
                update_interval: update_interval.unwrap_or(Duration::from_secs(2)),
                num_virtual_nodes: num_virtual_nodes.unwrap_or(4),
            };
            info!("starting");
            let c = chord::Chord::start(&host, cfg).await?;
            return c
                .start_loop(|key| async move {
                    info!("key event: {:?}", key);
                })
                .await;
        }
        Opt::Connect { host, bootstrap } => {
            let c = chord::Chord::connect(&host, &bootstrap).await?;
            return c
                .start_loop(|key| async move {
                    info!("key event: {:?}", key);
                })
                .await;
        }
        Opt::Query(x) => match query::query(&x).await {
            Ok(()) => {}
            Err(e) => error!("failed to query key: {}", e),
        },
        Opt::Quit { host } => control::quit(&host).await?,
        Opt::Add {
            host,
            value,
            is_key,
        } => control::add(&host, &value, is_key).await?,
        Opt::Get {
            host,
            value,
            is_key,
        } => control::get(&host, &value, is_key).await?,
    }
    Ok(())
}
