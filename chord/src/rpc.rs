use super::{Finger, Key};
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::VecDeque, future::Future, net::SocketAddr, result::Result as StdResult, sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Mutex},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Quit,
    Config,
    Ping,
    Node { which: Key, request: NodeRequest },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeRequest {
    /// Stop the node
    Notify(Finger),
    /// Add a key to the node
    AddKey(Key),
    /// Replicate a key to other nodes
    AddKeyReplicate(Key),
    /// Transfer the responisibility for a set of keys to a other node
    Contains(Key),
    /// Transfer the responisibility for a set of keys to a other node
    TransferKeys(Vec<Key>),
    /// Check if a key is within the right range according
    InRange {
        find: Key,
        this: Key,
    },
    /// Retrieve the successor of a node
    Successor,
    /// Stablize the node
    Stablize,
    FindClosestPredecessor(Key),
    FindSuccessor(Key),
}

pub type Response = StdResult<ResponseData, ResponseError>;

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseData {
    Quit,
    Notify,
    AddKey,
    AddKeyReplicate,
    TransferKeys,
    Pong,
    Contains(bool),
    Config(super::Config),
    Successor(Finger),
    Stablize {
        predecessor: Option<Finger>,
        successors: VecDeque<Finger>,
    },
    FindClosestPredecessor(Finger),
    FindSuccessor(Option<Finger>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseError {
    NoSuchNode,
    InvalidSizeKey,
    Other,
}

async fn send<S, T>(mut stream: S, v: &T) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let data = bincode::serialize(v)?;
    stream.write_u64_le(data.len() as u64).await?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
}
async fn recv<S, T>(mut stream: S) -> Result<T>
where
    S: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
    let len = stream.read_u64_le().await?;
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(bincode::deserialize(&buf)?)
}

async fn call(addr: &SocketAddr, req: Request, local: Option<&Local>) -> Result<Response> {
    if let Some(x) = local {
        if x.addr == *addr {
            let (send, recv) = oneshot::channel();
            x.send
                .send(LocalRequest {
                    request: req,
                    response: send,
                })
                .await
                .unwrap();
            return Ok(recv.await.unwrap());
        }
    }
    trace!("call: {:?}", req);
    let stream = TcpStream::connect(addr).await?;
    let mut stream = BufStream::new(stream);
    send(&mut stream, &req).await?;
    Ok(recv(&mut stream).await?)
}

pub async fn successor(finger: &Finger, local: Option<&Local>) -> Result<Finger> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Successor,
    };
    match call(&finger.addr, req, local).await? {
        Ok(ResponseData::Successor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn find_successor(
    finger: &Finger,
    key: Key,
    local: Option<&Local>,
) -> Result<Option<Finger>> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::FindSuccessor(key),
    };
    match call(&finger.addr, req, local).await? {
        Ok(ResponseData::FindSuccessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn find_closest_predecessor(
    finger: &Finger,
    key: Key,
    local: Option<&Local>,
) -> Result<Finger> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::FindClosestPredecessor(key),
    };
    match call(&finger.addr, req, local).await? {
        Ok(ResponseData::FindClosestPredecessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn notify(finger: &Finger, of: Finger, local: Option<&Local>) -> Result<()> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Notify(of),
    };
    match call(&finger.addr, req, local).await? {
        Ok(ResponseData::Notify) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn ping(addr: &SocketAddr, local: Option<&Local>) -> Result<()> {
    match call(addr, Request::Ping, local).await? {
        Ok(ResponseData::Pong) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn stabilize_info(
    finger: &Finger,
    local: Option<&Local>,
) -> Result<(Option<Finger>, VecDeque<Finger>)> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Stablize,
    };
    match call(&finger.addr, req, local).await? {
        Ok(ResponseData::Stablize {
            predecessor,
            successors,
        }) => Ok((predecessor, successors)),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn quit(addr: &SocketAddr, _local: Option<&Local>) -> Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let mut stream = BufStream::new(stream);
    let req = Request::Quit;
    send(&mut stream, &req).await?;
    recv::<_, Response>(&mut stream).await.ok();
    Ok(())
}

pub async fn config(addr: &SocketAddr, local: Option<&Local>) -> Result<super::Config> {
    match call(addr, Request::Config, local).await? {
        Ok(ResponseData::Config(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn transfer_keys(finger: &Finger, keys: Vec<Key>, local: Option<&Local>) -> Result<()> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::TransferKeys(keys),
        },
        local,
    )
    .await?
    {
        Ok(ResponseData::TransferKeys) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn add_key(finger: &Finger, key: Key, local: Option<&Local>) -> Result<()> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::AddKey(key),
        },
        local,
    )
    .await?
    {
        Ok(ResponseData::AddKey) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn contains_key(finger: &Finger, find: Key, local: Option<&Local>) -> Result<bool> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::Contains(find),
        },
        local,
    )
    .await?
    {
        Ok(ResponseData::Contains(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

#[derive(Debug)]
struct LocalRequest {
    request: Request,
    response: oneshot::Sender<Response>,
}

#[derive(Clone, Debug)]
pub struct Local {
    send: mpsc::Sender<LocalRequest>,
    addr: SocketAddr,
}

#[derive(Debug)]
pub struct Server {
    local_requests: Mutex<mpsc::Receiver<LocalRequest>>,
    local: Local,
    addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Self {
        let (send, recv) = mpsc::channel(128);
        Server {
            addr,
            local_requests: Mutex::new(recv),
            local: Local { send, addr },
        }
    }

    pub fn local(&self) -> Local {
        self.local.clone()
    }

    pub async fn handle<H, F>(&self, listener: TcpListener, handler: H) -> Result<()>
    where
        H: Fn(Request) -> F + Send + 'static + Sync,
        F: Future<Output = Result<Response>> + Send,
    {
        let handler = Arc::new(handler);
        let mut lock = self.local_requests.lock().await;
        loop {
            tokio::select! {
                x = listener.accept() => {
                    let (stream, addr) = x?;
                    trace!("incomming connection from: {}", addr);
                    let mut stream = BufStream::new(stream);
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let req: Request = recv(&mut stream).await?;
                        let res = handler(req).await?;
                        send(&mut stream, &res).await
                    });
                }
                x = lock.recv() => {
                    let x = x.unwrap();
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let res = handler(x.request).await?;
                        x.response.send(res).unwrap();
                        Result::<()>::Ok(())
                    });
                }
            }
        }
    }
}
