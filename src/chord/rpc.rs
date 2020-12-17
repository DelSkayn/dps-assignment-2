use serde::{Serialize,de::DeserializeOwned};
use tokio::{
    io::{AsyncReadExt,AsyncWriteExt,BufStream},
    net::{TcpStream, TcpListener},
};
use std::{
    collections::VecDeque,
    future::Future,
    result::Result as StdResult,
    sync::Arc,
};
use anyhow::Result;
use super::{Finger, Key};

#[derive(Serialize,Deserialize,Debug,Clone)]
pub enum RequestKind{
    Quit,
    Notify(Finger),
    Ping,
    Successor,
    Stablize,
    FindClosestPredecessor(Key),
    FindSuccessor(Key),
}

#[derive(Serialize,Deserialize,Debug)]
pub struct Request{
    pub id: Key,
    pub kind: RequestKind
}

pub type Response = StdResult<ResponseData,ResponseError>;

#[derive(Serialize,Deserialize,Debug)]
pub enum ResponseData{
    Quit,
    Notify,
    Pong,
    Successor(Finger),
    Stablize{
        predecessor: Option<Finger>,
        successors: VecDeque<Finger>,
    },
    FindClosestPredecessor(Finger),
    FindSuccessor(Option<Finger>),
}

#[derive(Serialize,Deserialize,Debug)]
pub enum ResponseError{
    NoSuchNode,
    InvalidSizeKey,
    Other,
}


async fn send<S,T>(mut stream: S,v: &T) -> Result<()>
where S: AsyncWriteExt + Unpin,
      T: Serialize
{
    let data = bincode::serialize(v)?;
    stream.write_u64_le(data.len() as u64).await?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
} async fn recv<S,T>(mut stream: S) -> Result<T>
where S: AsyncReadExt + Unpin,
      T: DeserializeOwned
{
    let len = stream.read_u64_le().await?;
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(bincode::deserialize(&buf)?)
}

async fn call(finger: &Finger, req: RequestKind) -> Result<Response>{
    trace!("call: [{}]:{:?}",finger.id,req);
    let stream = TcpStream::connect(finger.addr).await?;
    let mut stream = BufStream::new(stream);
    let req = Request{
        id: finger.id,
        kind: req,
    };
    send(&mut stream,&req).await?;
    Ok(recv(&mut stream).await?)
}

pub async fn successor(finger: &Finger) -> Result<Finger>{
    match call(finger,RequestKind::Successor).await?{
        Ok(ResponseData::Successor(x)) => Ok(x),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn find_successor(finger: &Finger, key: Key) -> Result<Option<Finger>>{
    match call(finger,RequestKind::FindSuccessor(key)).await?{
        Ok(ResponseData::FindSuccessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn find_closest_predecessor(finger: &Finger, key: Key) -> Result<Finger>{
    match call(finger,RequestKind::FindClosestPredecessor(key)).await?{
        Ok(ResponseData::FindClosestPredecessor(x)) => Ok(x),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn notify(finger: &Finger, of: Finger) -> Result<()>{
    match call(finger,RequestKind::Notify(of)).await?{
        Ok(ResponseData::Notify) => Ok(()),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn ping(finger: &Finger) -> Result<()>{
    match call(finger,RequestKind::Ping).await?{
        Ok(ResponseData::Pong) => Ok(()),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn stabilize_info(finger: &Finger) -> Result<(Option<Finger>,VecDeque<Finger>)>{
    match call(finger,RequestKind::Stablize).await?{
        Ok(ResponseData::Stablize{
            predecessor,
            successors
        }) => Ok((predecessor,successors)),
        x => panic!("invalid response: {:?}",x)
    }
}

pub async fn quit(finger: &Finger) -> Result<()>{
    let stream = TcpStream::connect(finger.addr).await?;
    let mut stream = BufStream::new(stream);
    let req = Request{
        id: finger.id,
        kind: RequestKind::Quit,
    };
    send(&mut stream,&req).await?;
    recv::<_,Response>(&mut stream).await.ok();
    Ok(())
}

pub async fn handle_rpc<H,F>(listener: TcpListener, handler: H) -> Result<()>
    where H: Fn(Request) -> F + Send + 'static+ Sync,
          F: Future<Output = Result<Response>> + Send
{
    let handler = Arc::new(handler);
    loop {
        let (stream, addr) = listener.accept().await?;
        trace!("incomming connection from: {}",addr);
        let mut stream = BufStream::new(stream);
        let handler = handler.clone();
        tokio::spawn(async move {
            let req: Request = recv(&mut stream).await?;
            let res = handler(req).await?;
            send(&mut stream, &res).await
        });
    }

}
