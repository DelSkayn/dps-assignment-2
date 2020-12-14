package chord

import (
	"net"
	"sync"
)

type Finger struct {
	ID   Key
	Addr *net.TCPAddr
}

type Fingers []*Finger

func (a Fingers) Len() int { return len(a) }
func (a Fingers) Insert(at int, finger *Finger) Fingers {
	if at == a.Len() {
		return append(a, finger)
	} else {
		res := append(a[:at+1], a[at:]...)
		res[at] = finger
		return res
	}
}
func (a Fingers) Remove(at int) Fingers {
	if at == a.Len()-1 {
		return a[:at]
	}
	return append(a[:at], a[at+1:]...)
}

type fingerTable struct {
	lock sync.Mutex
	data fingerTableData
}

type fingerTableData struct {
	ID            Key
	numSuccessors uint32
	bitsInKey     uint32

	fingers     Fingers
	predecessor *Finger
	successors  Fingers
}

func CreateFingerTable(ID Key, bitsInKey uint32, numSuccessors uint32, successor *Finger) fingerTable {
	return fingerTable{
		data: fingerTableData{
			numSuccessors: numSuccessors,
			bitsInKey:     bitsInKey,
			ID:            ID,
			fingers:       []*Finger{successor},
		},
	}
}

func (self *fingerTable) with(f func(*fingerTableData)) {
	self.lock.Lock()
	f(&self.data)
	self.lock.Unlock()
}

func (self *fingerTableData) lookupSuccessorFinger(find Key) int {
	i := 0
	for i = range self.fingers {
		ran := self.ID.To(&self.fingers[i].ID)
		if find.In(&ran) {
			break
		}
	}
	ran := self.ID.To(&self.fingers[i].ID)
	if find.In(&ran) {
		return i
	} else {
		return -1
	}
}

func (self *fingerTableData) append(finger *Finger) {
	self.appendFinger(finger)
	self.appendSuccessor(finger)
}

func (self *fingerTableData) appendSuccessor(finger *Finger) {
	prev := self.fingers[0]
	for i := 0; i < self.successors.Len(); i++ {
		ran := prev.To(&self.successors[i].ID)
		if finger.ID.In(ran) {
			if finger.ID.Cmp(&self.successors[i].ID) == 0 {
				return
			}
			self.successors.Insert(i, finger)
		}
	}
	if self.successors.Len() > int(self.numSuccessors) {
		self.successors = self.successors[:self.numSuccessors]
	}
}

func (self *fingerTableData) appendFinger(finger *Finger) {
	if self.fingers.Len() < int(self.bitsInKey) {
		// Not yet full
		lookup := self.lookupSuccessorFinger(finger.ID)
		if lookup < 0 {
			// No finger found which is a successor append finger
			self.fingers = append(self.fingers, finger)
		} else {
			if self.fingers[lookup].ID.Cmp(&finger.ID) == 0 {
				return
			} else {
				self.fingers = self.fingers.Insert(lookup, finger)
			}
		}
	} else {
		// Finger table already full, only refine fingers
		closestFingerKey := self.ID
		for i := 0; i < int(self.bitsInKey); i++ {
			newClosestFingerKey := self.ID.Next(uint(i))
			ran := self.ID.To(&newClosestFingerKey)
			if !finger.ID.In(&ran) {
				break
			}
			closestFingerKey = newClosestFingerKey
		}
		lookup := self.lookupSuccessorFinger(closestFingerKey)
		if lookup < 0 {
			// CORRECT?
			return
		}
		ran := closestFingerKey.To(&self.fingers[lookup].ID)
		if finger.ID.In(&ran) {
			// Finger is a closer successor so replace
			// Replace key
			self.fingers[lookup] = finger
		}
	}
}

// Remove entries associated with the key
// Should be used when a node has failed
func (self *fingerTableData) invalidateKey(ID Key) {
	for i := self.fingers.Len() - 1; i > 0; i-- {
		if self.fingers[i].ID.Cmp(&ID) == 0 {
			self.fingers = self.fingers.Remove(i)
		}
	}
	for i := self.successors.Len() - 1; i > 0; i-- {
		if self.successors[i].ID.Cmp(&ID) == 0 {
			self.successors = self.successors.Remove(i)
		}
	}
}

func (self *fingerTableData) findClosestPredecessor(ID Key) *Finger {
	ran := self.ID.To(&ID)
	for i := self.fingers.Len() - 1; i > 0; i-- {
		if self.fingers[i].ID.In(&ran) {
			return self.fingers[i]
		}
	}
	return nil
}
