use sha2::{Digest, Sha256};
use std::{fmt, net::SocketAddr};

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Serialize, Deserialize, Hash)]
pub struct Key(u128);

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#X}", self.0)
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Key({:#X})", self.0)
    }
}

impl Key {
    pub fn new(addr: &SocketAddr, virtual_id: u32, num_bits: u8) -> Self {
        let hash = Sha256::new()
            .chain(format!("{}", addr).as_bytes())
            .chain(virtual_id.to_le_bytes())
            .finalize();

        let mut data = [0u8; 16];
        hash.into_iter().enumerate().for_each(|(i, x)| {
            if i < 16 {
                data[i] = x;
            }
        });
        let data = u128::from_le_bytes(data);
        let m = 1 << num_bits as u128;
        Key(data % m)
    }

    pub fn from_bytes(data: &[u8], num_bits: u8) -> Self {
        let hash = sha2::Sha256::new().chain(data).finalize();
        let mut data = [0u8; 16];
        hash.into_iter().enumerate().for_each(|(i, x)| {
            if i < 16 {
                data[i] = x;
            }
        });
        let data = u128::from_le_bytes(data);
        let m = 1 << num_bits as u128;
        Key(data % m)
    }

    pub fn from_number(v: u128) -> Self {
        Key(v)
    }

    pub fn to(self, other: Self) -> KeyRange {
        KeyRange {
            from: self,
            to: other,
        }
    }

    pub fn within(&self, range: &KeyRange) -> bool {
        if *self == range.to {
            return true;
        }
        if range.from > range.to {
            *self > range.from || *self <= range.to
        } else {
            *self > range.from && *self <= range.to
        }
    }

    pub fn next(&self, next: u8, num_bits: u8) -> Self {
        let m = 1 << num_bits as u128;
        Key((self.0 + (1 << next)) % m)
    }
}

#[derive(Debug)]
pub struct KeyRange {
    pub from: Key,
    pub to: Key,
}
