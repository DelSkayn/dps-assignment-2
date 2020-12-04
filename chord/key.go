package chord

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"math"
	"math/big"
	"net"
)

type Key struct {
	Inner big.Int
}

type KeyRange struct {
	from *Key
	to   *Key
}

func CreateKey(addr *net.TCPAddr, virtualID uint32, numBits uint32) Key {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(addr); err != nil {
		panic(err)
	}
	b.WriteByte(byte(0xff & virtualID))
	b.WriteByte(byte(0xff & (virtualID >> 8)))
	b.WriteByte(byte(0xff & (virtualID >> 16)))
	b.WriteByte(byte(0xff & (virtualID >> 24)))
	hasher := sha1.New()
	if _, err := hasher.Write(b.Bytes()); err != nil {
		panic(err)
	}
	hash := hasher.Sum(nil)
	res := big.Int{}
	res.SetBytes(hash)

	mod := big.NewInt(2)
	mod.Exp(mod, big.NewInt(int64(numBits)), nil)

	res.Mod(&res, mod)

	return Key{Inner: res}
}

func (a *Key) Next(k uint) Key {
	slice := make([]byte, sha1.BlockSize)
	for i := 0; i < len(slice); i++ {
		slice[i] = 0xff
	}
	mod := big.NewInt(0).SetBytes(slice)
	offset := big.NewInt(2)
	offset.Lsh(offset, k)
	res := offset.Add(&a.Inner, offset)
	return Key{Inner: *res.And(res, mod)}
}

func (a *Key) Cmp(other *Key) int {
	return a.Inner.Cmp(&other.Inner)
}

func (a *Key) To(b *Key) KeyRange {
	return KeyRange{
		from: a,
		to:   b,
	}
}

// Calculates if the key is in the range [to,from)
func (a *Key) In(ran *KeyRange) bool {
	from := a.Cmp(ran.from)
	to := a.Cmp(ran.to)
	// Inclusive
	if to == 0 {
		return true
	}
	if ran.from.Cmp(ran.to) == 1 {
		return from == 1 || to == -1
	} else {
		return from == 1 && to == -1
	}
}

func (a *Key) Readable() string {
	res := big.Int{}
	res.SetBytes(a.Inner.Bytes())
	res.Mod(&res, big.NewInt(math.MaxInt64))
	return fmt.Sprintf("%X", res.Uint64())
}
