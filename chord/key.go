package chord

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"math/big"
	"net"
)

const (
	maxFingers = sha1.BlockSize * 8
)

type Key struct {
	inner big.Int
}

type KeyRange struct {
	from Key
	to   Key
}

func CreateKey(addr *net.TCPAddr, virtualID uint32) Key {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(addr)
	b.WriteByte(byte(0xff & virtualID))
	b.WriteByte(byte(0xff & (virtualID >> 8)))
	b.WriteByte(byte(0xff & (virtualID >> 16)))
	b.WriteByte(byte(0xff & (virtualID >> 24)))
	hasher := sha1.New()
	hasher.Write(b.Bytes())
	hash := hasher.Sum(nil)
	res := big.Int{}
	res.SetBytes(hash)
	return Key{
		inner: res,
	}
}

func (a *Key) Next(k uint) Key {
	slice := make([]byte, sha1.BlockSize)
	for i := 0; i < len(slice); i++ {
		slice[i] = 0xff
	}
	mod := big.NewInt(0).SetBytes(slice)
	offset := big.NewInt(2)
	offset.Lsh(offset, k)
	res := offset.Add(&a.inner, offset)
	return Key{
		inner: *res.And(res, mod),
	}
}

func (a *Key) Less(b *Key) bool {
	return a.inner.Cmp(&b.inner) == -1
}

func (a *Key) LessEqual(b *Key) bool {
	cmp := a.inner.Cmp(&b.inner)
	return cmp == -1 || cmp == 0
}

func (a *Key) Equal(b *Key) bool {
	return a.inner.Cmp(&b.inner) == 0
}

func (a *Key) to(b *Key) KeyRange {
	return KeyRange{
		from: *a,
		to:   *b,
	}
}

func (a *Key) in(b *KeyRange) bool {
	if b.from.Less(&b.to) {
		return b.from.Less(a) && a.LessEqual(&b.to)
	} else {
		return a.Less(&b.from) || b.to.LessEqual(a)
	}
}

/*
type Key [32]byte

func Create(addr *net.TCPAddr, virtualID uint32) Key {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(addr)
	b.WriteByte(byte(0xff & virtualID))
	b.WriteByte(byte(0xff & (virtualID >> 8)))
	b.WriteByte(byte(0xff & (virtualID >> 16)))
	b.WriteByte(byte(0xff & (virtualID >> 24)))
	hasher := sha256.New()
	hasher.Write(b.Bytes())
	res := hasher.Sum(nil)
	if len(res) != 32 {
		log.Panic("invalid size hash")
	}
	buf := [32]byte{}
	copy(buf[:], res)
	return buf
}

func (a *Key) Less(b *Key) bool {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (a *Key) LessEqual(b *Key) bool {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return true
}

func (a *Key) Equal(b *Key) bool {
	for i := 0; i < 32; i++ {
		if a[i] < b[i] {
			return false
		}
		if a[i] > b[i] {
			return false
		}
	}
	return true
}

func (a *Key) Mod(m uint64) uint64 {
	//todo
	panic("todo")
	return m
}
*/
