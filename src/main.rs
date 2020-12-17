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

mod query;

mod chord;
pub use chord::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "cdfs")]
enum Opt {
    Query {
        node: String,
        value: String,
        #[structopt(long = "virtual", default_value = "0")]
        virtual_node: u32,
        #[structopt(short = "k", long = "key")]
        is_key: bool,
        #[structopt(short = "b", long = "bits", default_value = "16")]
        num_bits: u8,
    },
    Start {
        host: String,
        bootstrap: Option<String>,
        #[structopt(short = "b", long = "bits", default_value = "16")]
        num_bits: u8,
        #[structopt(short = "s", long = "successors", default_value = "3")]
        num_successors: u32,
        #[structopt(long = "nvirtuals", default_value = "4")]
        num_virtual_nodes: u32,
        #[structopt(short = "i", long = "interval", default_value = "2s", parse(try_from_str = parse_duration))]
        update_interval: Duration,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    match opt {
        Opt::Start {
            host,
            bootstrap,
            num_bits,
            num_successors,
            update_interval,
            num_virtual_nodes,
        } => {
            let cfg = chord::Config {
                host,
                bootstrap,
                num_bits,
                num_successors,
                update_interval,
                num_virtual_nodes,
            };
            info!("starting");
            return chord::Chord::run(cfg).await;
        }
        Opt::Query {
            node,
            value,
            virtual_node,
            is_key,
            num_bits,
        } => match query::query(node, value, virtual_node, is_key, num_bits).await {
            Ok(()) => {}
            Err(e) => error!("failed to query key: {}", e),
        },
    }
    Ok(())
}
