package pubsub

import (
	"cloud.google.com/go/pubsub"
	_ "fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageToJSONObject(t *testing.T) {

	t.Run("should convert *pubsub.Message to JSON object (map[string]interface{})", func(t *testing.T) {

		jsonObj := `{"id":1}`
		msg := pubsub.Message{
			Data: []byte(jsonObj),
		}
		want := map[string]interface{}{
			"id": float64(1),
		}
		result, err := MessageToJSONObject(&msg)
		assert.Nil(t, err)
		assert.Equal(t, result, want)
	})
}
func TestMessageDataToJSONObject(t *testing.T) {

	t.Run("should convert *pubsub.Message.Data ([]byte) to JSON object (map[string]interface{})", func(t *testing.T) {

		jsonObj := `{"id":2}`
		msg := pubsub.Message{
			Data: []byte(jsonObj),
		}
		want := map[string]interface{}{
			"id": float64(2),
		}
		result, err := MessageDataToJSONObject(msg.Data)
		assert.Nil(t, err)
		assert.Equal(t, result, want)
	})
}
