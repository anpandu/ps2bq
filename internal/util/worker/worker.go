package worker

import (
	"sync"

	"cloud.google.com/go/pubsub"
	log "github.com/sirupsen/logrus"
)

type ProcessMessagesFn func([]*pubsub.Message, int) error

func DeployWorker(messageChan <-chan []*pubsub.Message, workerId int, fn ProcessMessagesFn, wg *sync.WaitGroup) {
	// var messages []interface{}
	for {
		messages, more := <-messageChan
		if !more {
			break
		} else {
			err := fn(messages, workerId)
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
	wg.Done()
}

func DeployWorkers(messageChan <-chan []*pubsub.Message, workerTotal int, fn ProcessMessagesFn, wg *sync.WaitGroup) {
	for idx := 0; idx < workerTotal; idx++ {
		wg.Add(1)
		go DeployWorker(messageChan, idx, fn, wg)
	}
}
