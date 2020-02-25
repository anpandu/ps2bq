package worker

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

type ProcessMessagesFn func([]interface{}, int) error

func DeployWorker(messageChan <-chan interface{}, msgsLength int, workerId int, fn ProcessMessagesFn, wg *sync.WaitGroup) {
	var messages []interface{}
	for {
		message, more := <-messageChan
		if !more {
			break
		}
		if message != "" {
			messages = append(messages, message)
			if len(messages) == msgsLength {
				err := fn(messages, workerId)
				if err != nil {
					log.Error(err.Error())
				}
				messages = []interface{}{}
			}
		}
	}
	wg.Done()
}

func DeployWorkers(messageChan <-chan interface{}, msgsLength int, workerTotal int, fn ProcessMessagesFn, wg *sync.WaitGroup) {
	for idx := 0; idx < workerTotal; idx++ {
		wg.Add(1)
		go DeployWorker(messageChan, msgsLength, idx, fn, wg)
	}
}
