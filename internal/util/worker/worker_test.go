package worker

import (
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

var (
	msg1 = pubsub.Message{
		Data: []byte(`{"id":1}`),
	}
	msg2 = pubsub.Message{
		Data: []byte(`{"id":2}`),
	}
	msg3 = pubsub.Message{
		Data: []byte(`{"id":3}`),
	}
	msgs                              = []*pubsub.Message{&msg1, &msg2, &msg3}
	sendPubSubMessagesAndCloseChannel = func(ch chan<- []*pubsub.Message, input []*pubsub.Message) {
		defer close(ch)
		ch <- input
	}
)

func TestDeployWorker(t *testing.T) {

	t.Run("A Worker should process 1 message (pubsub.Message)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan []*pubsub.Message)
		var msg = pubsub.Message{
			Data: []byte(`{"id":1}`),
		}
		var slowFunction = func(messages []*pubsub.Message, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			assert.Equal(t, messages[0], &msg)
			assert.Equal(t, messages[0].Data, msg.Data)
			assert.Equal(t, workerId, 666)
			return nil
		}
		go sendPubSubMessagesAndCloseChannel(channel, []*pubsub.Message{&msg})
		wg.Add(1)
		go DeployWorker(channel, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("A Worker should process 3 messages (pubsub.Message)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan []*pubsub.Message)
		var slowFunction = func(messages []*pubsub.Message, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 3)
			assert.Equal(t, workerId, 666)
			for i := 0; i < len(messages); i++ {
				assert.Equal(t, messages[i], msgs[i])
			}
			return nil
		}
		go sendPubSubMessagesAndCloseChannel(channel, msgs)
		wg.Add(1)
		go DeployWorker(channel, 666, slowFunction, &wg)
		wg.Wait()
	})
}

func BenchmarkDeployWorker(b *testing.B) {
	var wg sync.WaitGroup
	var channel = make(chan []*pubsub.Message)
	var slowFunction = func(messages []*pubsub.Message, workerId int) error {
		return nil
	}
	go func(ch chan<- []*pubsub.Message, input []*pubsub.Message) {
		defer close(ch)
		for i := 0; i < b.N; i++ {
			ch <- input
		}
	}(channel, msgs)
	wg.Add(1)
	go DeployWorker(channel, 666, slowFunction, &wg)
	wg.Wait()
}

func TestDeployWorkers(t *testing.T) {

	t.Run("Multiple Workers should process 3 messages each", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan []*pubsub.Message)
		var slowFunction = func(messages []*pubsub.Message, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, 3, len(messages))
			return nil
		}
		go func(ch chan []*pubsub.Message, msgs []*pubsub.Message) {
			defer close(ch)
			for i := 0; i < 3; i++ {
				ch <- msgs
			}
		}(channel, msgs)
		workersTotal := 3
		DeployWorkers(channel, workersTotal, slowFunction, &wg)
		wg.Wait()
	})
}
