use crate::chord;
use anyhow::{anyhow, Context, Result};
use sha2::Digest;
use std::{collections::HashSet, net::SocketAddr};
use structopt::StructOpt;
use tokio::net;


#[derive(Debug, StructOpt)]
pub enum Test{
    Query{
        bootstrap: String,
        times: usize,
    }
}

async fn test(test: Test){
}
