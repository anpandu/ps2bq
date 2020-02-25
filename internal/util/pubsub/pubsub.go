package pubsub

import (
	ps "cloud.google.com/go/pubsub"
	"encoding/json"
	// "reflect"
)

func MessageToJSONObject(message *ps.Message) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(message.Data, &result)
	return result, err
}

func MessageDataToJSONObject(messageData []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(messageData, &result)
	return result, err
}
