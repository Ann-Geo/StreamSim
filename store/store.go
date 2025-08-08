package store

import (
	"sync"
	"time"
)

type MessageStore struct {
	MsgAttrMap map[string]time.Time
	MapMutex   sync.Mutex
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		MsgAttrMap: make(map[string]time.Time),
	}
}

func (ms *MessageStore) Store(attribute string, timeStamp time.Time) {
	ms.MapMutex.Lock()
	defer ms.MapMutex.Unlock()
	ms.MsgAttrMap[attribute] = timeStamp
}

func (ms *MessageStore) Retrieve(attribute string) time.Time {
	ms.MapMutex.Lock()
	defer ms.MapMutex.Unlock()
	timeStamp, found := ms.MsgAttrMap[attribute]
	if found {
		//delete(ms.MsgAttrMap, attribute)
		return timeStamp
	}

	//zero time
	return time.Time{}
}
