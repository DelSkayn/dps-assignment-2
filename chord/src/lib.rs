#![allow(dead_code)]
#![allow(unused_imports)]

use anyhow::Result;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpSocket,
    sync::{mpsc, oneshot, Mutex},
    time,
};

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;

mod key;
pub mod rpc;
mod virtual_node;

pub use key::{Key, KeyRange};
pub use virtual_node::{Finger, FingerTable, VirtualNode};

#[derive(Debug)]
pub enum KeyEvent {
    Added(Key),
    Removed(Key),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub num_bits: u8,
    pub num_successors: u32,
    pub num_virtual_nodes: u32,
    pub update_interval: Duration,
}

#[derive(Debug)]
struct Inner {
    cfg: Config,
    resolved_host: SocketAddr,
    virtual_nodes: Vec<VirtualNode>,
    quit: Mutex<Option<oneshot::Sender<()>>>,
    rpc: rpc::Server,
}

#[derive(Debug)]
pub struct Chord {
    key_channel: mpsc::Receiver<KeyEvent>,
    inner: Arc<Inner>,
    quit: oneshot::Receiver<()>,
}

impl Chord {
    pub async fn connect(host: &str, bootstrap: &str) -> Result<Chord> {
        let host = tokio::net::lookup_host(host)
            .await?
            .next()
            .ok_or(anyhow!("failed to find an address for host"))?;

        info!("starting node on {}", host);

        let bootstrap = tokio::net::lookup_host(bootstrap)
            .await?
            .next()
            .ok_or(anyhow!("failed to find an address for host"))?;

        info!("connecting to {}", bootstrap);
        let config = rpc::config(&bootstrap, None).await?;
        loop {
            if let Some(chord) = Self::initialize_bootstrap(&config, host, bootstrap).await? {
                info!("connected!");
                return Ok(chord);
            }
            info!("could not find successor, waiting for network to stabilize");
            time::sleep(config.update_interval).await;
        }
    }

    pub async fn start(host: &str, cfg: Config) -> Result<Chord> {
        let host = tokio::net::lookup_host(host)
            .await?
            .next()
            .ok_or(anyhow!("failed to find an address for host"))?;

        info!("starting node on {}", host);

        Ok(Self::initialize_starter(cfg, host))
    }

    fn initialize_starter(cfg: Config, host: SocketAddr) -> Chord {
        let rpc = rpc::Server::new(host);
        let (key_send, key_recv) = mpsc::channel((cfg.num_virtual_nodes * 8) as usize);
        let cfg_borrow = &cfg;
        let mut keys: Vec<_> = (0..cfg.num_virtual_nodes)
            .map(|x| Key::new(&host, x, cfg_borrow.num_bits))
            .collect();
        keys.sort();
        let virtual_nodes: Vec<_> = (0..cfg.num_virtual_nodes)
            .map(|x| {
                let next_idx = (x + 1) % cfg_borrow.num_virtual_nodes as u32;
                let successor = Finger {
                    id: keys[next_idx as usize],
                    addr: host.clone(),
                };
                let this = Finger {
                    id: keys[x as usize],
                    addr: host.clone(),
                };
                VirtualNode::new(this, successor, cfg_borrow, key_send.clone(), rpc.local())
            })
            .collect();
        let (sender, recv) = oneshot::channel();
        let inner = Inner {
            quit: Mutex::new(Some(sender)),
            cfg,
            resolved_host: host,
            virtual_nodes,
            rpc,
        };
        Chord {
            key_channel: key_recv,
            quit: recv,
            inner: Arc::new(inner),
        }
    }

    async fn initialize_bootstrap(
        cfg: &Config,
        host: SocketAddr,
        bootstrap: SocketAddr,
    ) -> Result<Option<Chord>> {
        let server = rpc::Server::new(host);
        let (key_send, key_recv) = mpsc::channel((cfg.num_virtual_nodes * 8) as usize);
        let cfg_borrow = &cfg;
        let mut keys: Vec<_> = (0..cfg.num_virtual_nodes)
            .map(|x| Key::new(&host, x, cfg_borrow.num_bits))
            .collect();
        keys.sort();
        let bootstrap = Finger {
            id: Key::new(&bootstrap, 0, cfg.num_bits),
            addr: bootstrap.clone(),
        };
        let mut virtual_nodes = Vec::new();
        for k in keys.iter() {
            let successor = rpc::find_successor(&bootstrap, *k, None).await?;
            if let Some(x) = successor {
                if x.id == *k {
                    bail!("node with id {} already in the network", x.id)
                }
                virtual_nodes.push(VirtualNode::new(
                    Finger { id: *k, addr: host },
                    x,
                    &cfg,
                    key_send.clone(),
                    server.local(),
                ));
            } else {
                return Ok(None);
            }
        }
        let (sender, recv) = oneshot::channel();
        let inner = Inner {
            quit: Mutex::new(Some(sender)),
            cfg: cfg.clone(),
            resolved_host: host,
            virtual_nodes,
            rpc: server,
        };
        Ok(Some(Chord {
            key_channel: key_recv,
            quit: recv,
            inner: Arc::new(inner),
        }))
    }

    pub async fn start_loop<F, R>(self, f: F) -> Result<()>
    where
        F: Fn(KeyEvent) -> R + Send + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        for i in 0..self.inner.virtual_nodes.len() {
            let clone = self.inner.clone();
            tokio::spawn(async move { clone.virtual_nodes[i].stabilize().await });
            let clone = self.inner.clone();
            tokio::spawn(async move { clone.virtual_nodes[i].fix_fingers().await });
        }
        let mut key_channel = self.key_channel;
        let handle = tokio::spawn(async move {
            loop {
                if let Some(x) = key_channel.recv().await {
                    tokio::spawn(f(x));
                } else {
                    return;
                }
            }
        });

        tokio::select! {
            _ = self.quit =>{
                info!("quit requested, exiting!");
                return Ok(())
            }
            x = self.inner.clone().run_loop() => {
                return x;
            }
            _ = handle => {
                bail!("key handle loop quit unexpectedly!");
            }
        }
    }
}

impl Inner {
    async fn run_loop(self: Arc<Self>) -> Result<()> {
        let socket = if self.resolved_host.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket.bind(self.resolved_host)?;
        let listener = socket.listen(128)?;
        let this = self.clone();
        self.rpc
            .handle(listener, move |req| {
                let this = this.clone();
                async move {
                    let res = match req {
                        rpc::Request::Ping => Ok(rpc::ResponseData::Pong),
                        rpc::Request::Quit => {
                            this.quit.lock().await.take().map(|x| x.send(()));
                            Ok(rpc::ResponseData::Quit)
                        }
                        rpc::Request::Config => Ok(rpc::ResponseData::Config(this.cfg.clone())),
                        rpc::Request::Node { which, request } => {
                            let node = match this
                                .virtual_nodes
                                .binary_search_by_key(&which, |x| x.this.id)
                            {
                                Ok(x) => &this.virtual_nodes[x],
                                Err(_) => return Ok(Err(rpc::ResponseError::NoSuchNode)),
                            };
                            match request {
                                rpc::NodeRequest::Stablize => {
                                    let (predecessor, successors) = node.get_stablize_info().await;
                                    Ok(rpc::ResponseData::Stablize {
                                        predecessor,
                                        successors,
                                    })
                                }
                                rpc::NodeRequest::Notify(predecessor) => {
                                    node.notify(predecessor.clone()).await;
                                    Ok(rpc::ResponseData::Notify)
                                }
                                rpc::NodeRequest::Successor => {
                                    Ok(rpc::ResponseData::Successor(node.get_successor().await))
                                }
                                rpc::NodeRequest::FindClosestPredecessor(x) => {
                                    let res = node.find_closest_predecessor(x).await;
                                    Ok(rpc::ResponseData::FindClosestPredecessor(res))
                                }
                                rpc::NodeRequest::FindSuccessor(x) => {
                                    let res = node.find_successor(x).await;
                                    Ok(rpc::ResponseData::FindSuccessor(res))
                                }
                                rpc::NodeRequest::TransferKeys(x) => {
                                    node.transver_keys(x).await;
                                    Ok(rpc::ResponseData::TransferKeys)
                                }
                                rpc::NodeRequest::AddKey(key) => {
                                    node.add_key(key).await;
                                    Ok(rpc::ResponseData::AddKey)
                                }
                                rpc::NodeRequest::Contains(key) => {
                                    let res = node.contains_key(key).await;
                                    Ok(rpc::ResponseData::Contains(res))
                                }
                                rpc::NodeRequest::Info => {
                                    let res = node.get_info().await;
                                    Ok(rpc::ResponseData::Info(res))
                                }
                            }
                        }
                    };
                    Ok(res)
                }
            })
            .await
    }
}
