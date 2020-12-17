use anyhow::Result;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpSocket,
    sync::{oneshot, Mutex},
};

mod key;
pub mod rpc;
mod virtual_node;

pub use key::Key;
pub use virtual_node::{Finger, FingerTable, VirtualNode};

#[derive(Debug)]
pub struct Config {
    pub host: String,
    pub bootstrap: Option<String>,
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
}

#[derive(Debug)]
pub struct Chord {
    inner: Arc<Inner>,
    quit: oneshot::Receiver<()>,
}

impl Chord {
    pub async fn run(cfg: Config) -> Result<()> {
        let mut host = None;
        for hosts in tokio::net::lookup_host(cfg.host.clone()).await? {
            host = Some(hosts);
        }
        if host.is_none() {
            bail!("failed to find an address for host")
        }
        let host = host.unwrap();

        let mut bootstrap = None;
        if let Some(x) = cfg.bootstrap.clone() {
            for bootstraps in tokio::net::lookup_host(x).await? {
                bootstrap = Some(bootstraps);
            }
            if bootstrap.is_none() {
                bail!("failed to find an address for bootstrap")
            }
        }

        let chord = if let Some(x) = bootstrap {
            Self::initialize_bootstrap(cfg, host, x).await?
        } else {
            Self::initialize_starter(cfg, host).await
        };
        chord.start().await?;
        Ok(())
    }

    async fn initialize_starter(cfg: Config, host: SocketAddr) -> Chord {
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
                VirtualNode::new(this, successor, cfg_borrow)
            })
            .collect();
        let (sender, recv) = oneshot::channel();
        let inner = Inner {
            quit: Mutex::new(Some(sender)),
            cfg,
            resolved_host: host,
            virtual_nodes,
        };
        Chord {
            quit: recv,
            inner: Arc::new(inner),
        }
    }

    async fn initialize_bootstrap(
        cfg: Config,
        host: SocketAddr,
        bootstrap: SocketAddr,
    ) -> Result<Chord> {
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
            let successor = rpc::find_successor(&bootstrap, *k).await?;
            if let Some(x) = successor {
                if x.id == *k {
                    bail!("node with id {} already in the network", x.id)
                }
                virtual_nodes.push(VirtualNode::new(Finger { id: *k, addr: host }, x, &cfg));
            } else {
                bail!("could not find successor!")
            }
        }
        let (sender, recv) = oneshot::channel();
        let inner = Inner {
            quit: Mutex::new(Some(sender)),
            cfg,
            resolved_host: host,
            virtual_nodes,
        };
        Ok(Chord {
            quit: recv,
            inner: Arc::new(inner),
        })
    }

    pub async fn start(self) -> Result<()> {
        for i in 0..self.inner.virtual_nodes.len() {
            let clone = self.inner.clone();
            tokio::spawn(async move { clone.virtual_nodes[i].stabilize().await });
            let clone = self.inner.clone();
            tokio::spawn(async move { clone.virtual_nodes[i].fix_fingers().await });
        }
        tokio::select! {
            _ = self.quit =>{
                info!("quit requested, exiting!");
                return Ok(())
            }
            x = self.inner.clone().run_loop() => {
                error!("run loop quit unexpectedly");
                return x;
            }
        }
    }
}

impl Inner {
    async fn run_loop(self: Arc<Self>) -> Result<()> {
        let socket = TcpSocket::new_v4()?;
        socket.bind(self.resolved_host)?;
        let listener = socket.listen(128)?;
        let this = self.clone();
        rpc::handle_rpc(listener, move |req| {
            let this = this.clone();
            async move {
                let node = match this
                    .virtual_nodes
                    .binary_search_by_key(&req.id, |x| x.this.id)
                {
                    Ok(x) => &this.virtual_nodes[x],
                    Err(_) => return Ok(Err(rpc::ResponseError::NoSuchNode)),
                };
                let res: rpc::Response = match req.kind {
                    rpc::RequestKind::Quit => {
                        this.quit.lock().await.take().map(|x| x.send(()));
                        Ok(rpc::ResponseData::Quit)
                    }
                    rpc::RequestKind::Ping => Ok(rpc::ResponseData::Pong),
                    rpc::RequestKind::Stablize => {
                        let (predecessor, successors) = node.get_stablize_info().await;
                        Ok(rpc::ResponseData::Stablize {
                            predecessor,
                            successors,
                        })
                    }
                    rpc::RequestKind::Notify(predecessor) => {
                        node.notify(predecessor.clone()).await;
                        Ok(rpc::ResponseData::Notify)
                    }
                    rpc::RequestKind::Successor => {
                        Ok(rpc::ResponseData::Successor(node.get_successor().await))
                    }
                    rpc::RequestKind::FindClosestPredecessor(x) => {
                        let res = node.find_closest_predecessor(x).await;
                        Ok(rpc::ResponseData::FindClosestPredecessor(res))
                    }
                    rpc::RequestKind::FindSuccessor(x) => {
                        let res = node.find_successor(x).await;
                        Ok(rpc::ResponseData::FindSuccessor(res))
                    }
                };
                Ok(res)
            }
        })
        .await
    }
}
