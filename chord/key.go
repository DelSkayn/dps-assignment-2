package chord

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"net"

	log "github.com/sirupsen/logrus"
)

type Key struct {
	inner uint64
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
	if len(hash) != 8 {
		log.Panic("invalid size hash")
	}
	res := binary.LittleEndian.Uint64(hash)
	return Key{
		inner: res,
	}
}

func (a *Key) Less(b *Key) bool {
	return a.inner < b.inner
}

func (a *Key) LessEqual(b *Key) bool {
	return a.inner <= b.inner
}

func (a *Key) Equal(b *Key) bool {
	return a.inner == b.inner
}

func (a *Key) Mod(m uint64) uint64 {
	return a.inner % m
}

func (a *Key) to(b *Key) KeyRange {
	return KeyRange{
		from: *a,
		to:   *b,
	}
}

func (a *Key) in(b *KeyRange) bool {
	if b.from.inner < b.to.inner {
		return b.from.inner <= a.inner && a.inner <= b.to.inner
	} else {
		return b.from.inner >= a.inner || a.inner >= b.to.inner
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
