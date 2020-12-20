use rand::Rng;
use std::{collections::VecDeque, fmt, mem, net::SocketAddr, time::Duration};
use tokio::{sync::Mutex, time};

use super::{rpc, Config, Key, KeyRange};

#[derive(Debug)]
pub struct KeySet(Vec<Key>);

impl KeySet {
    pub fn new() -> Self {
        KeySet(Vec::new())
    }

    pub fn insert(&mut self, k: Key) -> bool {
        match self.0.binary_search(&k) {
            Ok(_) => false,
            Err(x) => {
                self.0.insert(x, k);
                true
            }
        }
    }

    pub fn append(&mut self, keys: Vec<Key>) {
        for k in keys.into_iter() {
            match self.0.binary_search(&k) {
                Ok(_) => {}
                Err(x) => {
                    self.0.insert(x, k);
                }
            }
        }
    }

    pub fn remove_not_in_range(&mut self, ran: &KeyRange) -> Vec<Key> {
        let mut res = Vec::new();
        self.0.retain(|x| {
            if x.within(ran) {
                true
            } else {
                res.push(*x);
                false
            }
        });
        res
    }

    pub fn contains(&self, k: Key) -> bool {
        self.0.binary_search(&k).is_ok()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Finger {
    pub id: Key,
    pub addr: SocketAddr,
}

impl fmt::Display for Finger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}@{}]", self.id, self.addr)
    }
}

pub struct FingerTable {
    fingers: Vec<Option<Finger>>,
    predecessor: Option<Finger>,
    successors: VecDeque<Finger>,
}

impl fmt::Debug for FingerTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(x) = self.predecessor.as_ref() {
            write!(f, "({}<-|", x)?;
        } else {
            write!(f, "(None<-|")?;
        }
        for succ in self.successors.iter() {
            write!(f, "->{}", succ)?;
        }
        write!(f, ")\n{{\n")?;
        for finger in self.fingers.iter() {
            if let Some(x) = finger.as_ref() {
                writeln!(f, "\t{},", x)?;
            } else {
                writeln!(f, "\tNone,")?;
            }
        }
        write!(f, "}}\n")
    }
}

#[derive(Debug)]
pub struct VirtualNode {
    interval: Duration,
    num_bits: u8,
    num_successors: u32,
    pub this: Finger,
    pub table: Mutex<FingerTable>,
    pub key_set: Mutex<KeySet>,
}

impl VirtualNode {
    pub fn new(this: Finger, successor: Finger, cfg: &Config) -> Self {
        let fingers = vec![None; cfg.num_bits as usize - 1];
        VirtualNode {
            interval: cfg.update_interval,
            this,
            num_bits: cfg.num_bits,
            num_successors: cfg.num_successors,
            table: Mutex::new(FingerTable {
                fingers,
                predecessor: None,
                successors: VecDeque::from(vec![successor]),
            }),
            key_set: Mutex::new(KeySet::new()),
        }
    }

    pub async fn get_successor(&self) -> Finger {
        let lock = self.table.lock().await;
        if let Some(x) = lock.successors.front().cloned() {
            return x;
        }
        if let Some(x) = lock.fingers.iter().find_map(|x| x.as_ref()) {
            return x.clone();
        }
        panic!("Lost all successors, cannot continue!")
    }

    pub async fn invalidate_successor(&self, finger: &Finger) {
        let mut lock = self.table.lock().await;
        if let Some(x) = lock.successors.front() {
            if x.id == finger.id {
                lock.successors.pop_front();
            }
        }
    }

    pub async fn set_successors(&self, successors: VecDeque<Finger>) {
        self.table.lock().await.successors = successors;
    }

    pub async fn get_stablize_info(&self) -> (Option<Finger>, VecDeque<Finger>) {
        let lock = self.table.lock().await;
        (lock.predecessor.clone(), lock.successors.clone())
    }

    pub async fn add_key(&self, key: Key) {
        if !self.key_set.lock().await.insert(key) {
            warn!("key added to note was already present!")
        }
    }

    pub async fn contains_key(&self, key: Key) -> bool {
        self.key_set.lock().await.contains(key)
    }

    pub async fn transver_keys(&self, key: Vec<Key>) {
        self.key_set.lock().await.append(key)
    }

    pub async fn insert_finger(&self, finger: Finger) {
        let mut lock = self.table.lock().await;
        for i in 0..lock.fingers.len() as u8 {
            let finger_key = self.this.id.next(i + 1, self.num_bits);
            if finger.id.within(&self.this.id.to(finger_key)) && finger.id != finger_key {
                break;
            }
            if let Some(x) = lock.fingers[i as usize].as_ref() {
                if finger.id.within(&self.this.id.to(x.id)) {
                    lock.fingers[i as usize] = Some(finger.clone());
                }
            } else {
                lock.fingers[i as usize] = Some(finger.clone());
            }
        }
    }

    pub async fn invalidate_node(&self, id: Key) {
        let mut lock = self.table.lock().await;
        for i in 0..lock.fingers.len() {
            if let Some(x) = lock.fingers[i].as_ref() {
                if x.id == id {
                    lock.fingers[i] = None
                }
            }
        }
    }

    pub async fn find_closest_predecessor(&self, to: Key) -> Finger {
        'retry: loop {
            let lock = self.table.lock().await;
            let ran = self.this.id.to(to);
            for finger in lock.fingers.iter().rev() {
                if let Some(x) = finger.as_ref() {
                    if x.id.within(&ran) {
                        let x = x.clone();
                        mem::drop(lock);
                        if let Ok(_) = rpc::ping(&x).await {
                            return x;
                        } else {
                            self.invalidate_node(x.id).await;
                            continue 'retry;
                        }
                    }
                }
            }
            let succ = lock.successors.front().unwrap();
            if succ.id.within(&ran) {
                return succ.clone();
            }
            return self.this.clone();
        }
    }

    pub async fn find_successor(&self, key: Key) -> Option<Finger> {
        let succ = self.get_successor().await;
        if key.within(&self.this.id.to(succ.id)) {
            return Some(succ);
        }
        let mut closest = self.find_closest_predecessor(key).await;
        let mut succ = match rpc::successor(&closest).await {
            Ok(x) => x,
            Err(_) => {
                self.invalidate_node(succ.id).await;
                return None;
            }
        };
        while !key.within(&closest.id.to(succ.id)) {
            closest = match rpc::find_closest_predecessor(&closest, key).await {
                Ok(x) => x,
                Err(_) => {
                    self.invalidate_node(closest.id).await;
                    return None;
                }
            };
            let tmp = match rpc::successor(&closest).await {
                Ok(x) => x,
                Err(_) => {
                    self.invalidate_node(closest.id).await;
                    return None;
                }
            };
            if tmp.id == succ.id {
                return Some(succ);
            }
            succ = tmp
        }
        return Some(succ);
    }

    pub async fn notify(&self, predecessor: Finger) {
        let mut lock = self.table.lock().await;
        if let Some(x) = lock.predecessor.clone() {
            // Avoid holding onto the lock across an rpc call:
            mem::drop(lock);
            if let Ok(_) = rpc::ping(&x).await {
                let mut lock = self.table.lock().await;
                let range = lock.predecessor.as_ref().unwrap().id.to(self.this.id);
                if predecessor.id.within(&range) {
                    lock.predecessor = Some(predecessor);
                }
            } else {
                let mut lock = self.table.lock().await;
                lock.predecessor = Some(predecessor);
            }
        } else {
            lock.predecessor = Some(predecessor);
            mem::drop(lock);
        }
        let pred = self.table.lock().await.predecessor.clone();
        if let Some(x) = pred {
            let range = x.id.to(self.this.id);
            let keys = self.key_set.lock().await.remove_not_in_range(&range);
            if !keys.is_empty() {
                match rpc::transfer_keys(&x, keys.clone()).await {
                    Ok(()) => {}
                    Err(_) => {
                        self.table.lock().await.predecessor = None;
                        self.key_set.lock().await.append(keys);
                    }
                }
            }
        }
    }

    pub async fn stabilize(&self) {
        let mut interval = time::interval(self.interval);
        loop {
            let succ = self.get_successor().await;
            let (new_succ, mut successors) = match rpc::stabilize_info(&succ).await {
                Ok(x) => x,
                Err(_) => {
                    self.invalidate_successor(&succ).await;
                    continue;
                }
            };
            // Successor cannot be its own predecessor
            successors.push_front(succ.clone());
            if successors.len() > self.num_successors as usize {
                successors.pop_back();
            }
            self.set_successors(successors).await;
            let range = self.this.id.to(succ.id);
            if let Some(new_succ) = new_succ {
                if new_succ.id.within(&range) {
                    if let Ok((_, mut successors)) = rpc::stabilize_info(&new_succ).await {
                        successors.push_front(new_succ);
                        if successors.len() > self.num_successors as usize {
                            successors.pop_back();
                        }
                        self.set_successors(successors).await;
                    }
                }
            }
            let succ = self.get_successor().await;
            rpc::notify(&succ, self.this.clone()).await.ok();
            debug!("{:#?}", self);
            let random_interval = self.interval.div_f32(2.0);
            let random_interval = random_interval.mul_f32(rand::thread_rng().gen());
            time::sleep(random_interval).await;
            interval.tick().await;
        }
    }

    pub async fn fix_fingers(&self) {
        let mut interval = time::interval(self.interval);
        loop {
            let pick = rand::thread_rng().gen_range(0, self.num_bits - 1);
            let key = self.this.id.next(pick + 1, self.num_bits);
            let finger = self.table.lock().await.fingers[pick as usize].clone();
            if let Some(x) = finger {
                if let Err(_) = rpc::ping(&x).await {
                    self.invalidate_node(x.id).await;
                }
            }
            if let Some(x) = self.find_successor(key).await {
                self.insert_finger(x).await;
            }
            let random_interval = self.interval.div_f32(2.0);
            let random_interval = random_interval.mul_f32(rand::thread_rng().gen());
            time::sleep(random_interval).await;
            interval.tick().await;
        }
    }
}
