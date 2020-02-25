package worker

import (
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

var (
	texts = []string{
		"hello",
		"cruel",
		"world",
	}
	sendStringsAndCloseChannel = func(ch chan<- interface{}, input []string) {
		defer close(ch)
		for _, e := range input {
			ch <- e
		}
	}
	sendIntsAndCloseChannel = func(ch chan<- interface{}, input []int) {
		defer close(ch)
		for _, e := range input {
			ch <- e
		}
	}
	sendPubSubMessagesAndCloseChannel = func(ch chan<- interface{}, input []*pubsub.Message) {
		defer close(ch)
		for _, e := range input {
			ch <- e
		}
	}
	sendMultipleVarsAndCloseChannel = func(ch chan<- interface{}, input []interface{}) {
		defer close(ch)
		for _, e := range input {
			ch <- e
		}
	}
)

func TestDeployWorker(t *testing.T) {

	t.Run("A Worker should process 1 message (string)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var text = "a string"
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			assert.Equal(t, messages[0], text)
			assert.Equal(t, workerId, 666)
			return nil
		}
		go sendStringsAndCloseChannel(channel, []string{text})
		wg.Add(1)
		go DeployWorker(channel, 1, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("A Worker should process 1 message (int)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var number = 999
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			assert.Equal(t, messages[0], number)
			assert.Equal(t, workerId, 666)
			return nil
		}
		go sendIntsAndCloseChannel(channel, []int{number})
		wg.Add(1)
		go DeployWorker(channel, 1, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("A Worker should process 1 message (pubsub.Message)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msg = pubsub.Message{
			Data: []byte(`{"id":1}`),
		}
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			assert.Equal(t, messages[0], &msg)
			assert.Equal(t, messages[0].(*pubsub.Message), &msg)
			assert.Equal(t, messages[0].(*pubsub.Message).Data, msg.Data)
			assert.Equal(t, workerId, 666)
			return nil
		}
		go sendPubSubMessagesAndCloseChannel(channel, []*pubsub.Message{&msg})
		wg.Add(1)
		go DeployWorker(channel, 1, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("A Worker should process 3 messages (string)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 3
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), msgsLength)
			assert.Equal(t, workerId, 666)
			for i := 0; i < len(messages); i++ {
				assert.Equal(t, messages[i], texts[i])
			}
			return nil
		}
		go sendStringsAndCloseChannel(channel, texts)
		wg.Add(1)
		go DeployWorker(channel, msgsLength, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("A Worker should process 3 messages (any)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 3
		var mixed = []interface{}{
			"hello",
			123,
			"world",
		}
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), msgsLength)
			assert.Equal(t, workerId, 666)
			for i := 0; i < len(messages); i++ {
				assert.Equal(t, messages[i], mixed[i])
			}
			return nil
		}
		go sendMultipleVarsAndCloseChannel(channel, mixed)
		wg.Add(1)
		go DeployWorker(channel, msgsLength, 666, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("Multiple Workers should process 1 messages each", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 1
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			return nil
		}
		go sendStringsAndCloseChannel(channel, texts)
		wg.Add(1)
		go DeployWorker(channel, msgsLength, 666, slowFunction, &wg)
		wg.Add(1)
		go DeployWorker(channel, msgsLength, 777, slowFunction, &wg)
		wg.Add(1)
		go DeployWorker(channel, msgsLength, 888, slowFunction, &wg)
		wg.Wait()
	})
}

func BenchmarkDeployWorker(b *testing.B) {
	var wg sync.WaitGroup
	var channel = make(chan interface{})
	var msgsLength = 3
	var slowFunction = func(messages []interface{}, workerId int) error {
		return nil
	}
	go func(ch chan<- interface{}, input string) {
		defer close(ch)
		for i := 0; i < b.N; i++ {
			ch <- input
		}
	}(channel, texts[0])
	wg.Add(1)
	go DeployWorker(channel, msgsLength, 666, slowFunction, &wg)
	wg.Wait()
}

func TestDeployWorkers(t *testing.T) {

	t.Run("Multiple Workers should process 1 messages each", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 1
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, len(messages), 1)
			return nil
		}
		go sendStringsAndCloseChannel(channel, texts)
		workersTotal := 3
		DeployWorkers(channel, msgsLength, workersTotal, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("Multiple Workers should process 3 messages each", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 3
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			return nil
		}
		newTexts := []string{
			"uno",
			"dos",
			"tres",
			"quatro",
			"cinco",
			"seis",
		}
		go sendStringsAndCloseChannel(channel, newTexts)
		workersTotal := 2
		DeployWorkers(channel, msgsLength, workersTotal, slowFunction, &wg)
		wg.Wait()
	})

	t.Run("Multiple Workers should process 3 messages each (mixed)", func(t *testing.T) {
		var wg sync.WaitGroup
		var channel = make(chan interface{})
		var msgsLength = 3
		var slowFunction = func(messages []interface{}, workerId int) error {
			time.Sleep(20 * time.Millisecond)
			return nil
		}
		var mixed = []interface{}{
			"hello",
			123,
			"world",
			"never",
			456,
			"mind",
		}
		go sendMultipleVarsAndCloseChannel(channel, mixed)
		workersTotal := 2
		DeployWorkers(channel, msgsLength, workersTotal, slowFunction, &wg)
		wg.Wait()
	})
}
