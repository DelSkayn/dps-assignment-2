use super::{Finger, Key};
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::VecDeque, future::Future, net::SocketAddr, result::Result as StdResult, sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
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

async fn call(addr: &SocketAddr, req: Request) -> Result<Response> {
    trace!("call: {:?}", req);
    let stream = TcpStream::connect(addr).await?;
    let mut stream = BufStream::new(stream);
    send(&mut stream, &req).await?;
    Ok(recv(&mut stream).await?)
}

pub async fn successor(finger: &Finger) -> Result<Finger> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Successor,
    };
    match call(&finger.addr, req).await? {
        Ok(ResponseData::Successor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn find_successor(finger: &Finger, key: Key) -> Result<Option<Finger>> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::FindSuccessor(key),
    };
    match call(&finger.addr, req).await? {
        Ok(ResponseData::FindSuccessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn find_closest_predecessor(finger: &Finger, key: Key) -> Result<Finger> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::FindClosestPredecessor(key),
    };
    match call(&finger.addr, req).await? {
        Ok(ResponseData::FindClosestPredecessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn notify(finger: &Finger, of: Finger) -> Result<()> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Notify(of),
    };
    match call(&finger.addr, req).await? {
        Ok(ResponseData::Notify) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn ping(addr: &SocketAddr) -> Result<()> {
    match call(addr, Request::Ping).await? {
        Ok(ResponseData::Pong) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn stabilize_info(finger: &Finger) -> Result<(Option<Finger>, VecDeque<Finger>)> {
    let req = Request::Node {
        which: finger.id,
        request: NodeRequest::Stablize,
    };
    match call(&finger.addr, req).await? {
        Ok(ResponseData::Stablize {
            predecessor,
            successors,
        }) => Ok((predecessor, successors)),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn quit(addr: &SocketAddr) -> Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let mut stream = BufStream::new(stream);
    let req = Request::Quit;
    send(&mut stream, &req).await?;
    recv::<_, Response>(&mut stream).await.ok();
    Ok(())
}

pub async fn config(addr: &SocketAddr) -> Result<super::Config> {
    match call(addr, Request::Config).await? {
        Ok(ResponseData::Config(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn transfer_keys(finger: &Finger, keys: Vec<Key>) -> Result<()> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::TransferKeys(keys),
        },
    )
    .await?
    {
        Ok(ResponseData::TransferKeys) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn add_key(finger: &Finger, key: Key) -> Result<()> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::AddKey(key),
        },
    )
    .await?
    {
        Ok(ResponseData::AddKey) => Ok(()),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn contains_key(finger: &Finger, find: Key) -> Result<bool> {
    match call(
        &finger.addr,
        Request::Node {
            which: finger.id,
            request: NodeRequest::Contains(find),
        },
    )
    .await?
    {
        Ok(ResponseData::Contains(x)) => Ok(x),
        x => panic!("invalid response: {:?}", x),
    }
}

pub async fn handle_rpc<H, F>(listener: TcpListener, handler: H) -> Result<()>
where
    H: Fn(Request) -> F + Send + 'static + Sync,
    F: Future<Output = Result<Response>> + Send,
{
    let handler = Arc::new(handler);
    loop {
        let (stream, addr) = listener.accept().await?;
        trace!("incomming connection from: {}", addr);
        let mut stream = BufStream::new(stream);
        let handler = handler.clone();
        tokio::spawn(async move {
            let req: Request = recv(&mut stream).await?;
            let res = handler(req).await?;
            send(&mut stream, &res).await
        });
    }
}
