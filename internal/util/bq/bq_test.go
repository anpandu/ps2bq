package bq

import (
	"encoding/json"
	_ "fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

var (
	rows []map[string]interface{}
)

func TestCustomRow(t *testing.T) {

	t.Run("Save() should return map[string]bigquery.Value", func(t *testing.T) {
		rows := []map[string]interface{}{
			{"id": 1, "name": "Cosmo"},
			{"id": 2, "name": "Wanda"},
		}
		customRows := []CustomRow{
			{valMap: rows[0]},
			{valMap: rows[1]},
		}
		for i := 0; i < len(customRows); i++ {
			customRow := customRows[i]
			saveResult, insertID, err := customRow.Save()
			assert.Equal(t, reflect.TypeOf(saveResult).String(), "map[string]bigquery.Value")
			assert.Equal(t, saveResult["id"], customRow.valMap["id"])
			assert.Equal(t, saveResult["name"], customRow.valMap["name"])
			assert.Equal(t, insertID, "", "insertID should be empty string")
			assert.Nil(t, err)
		}
	})
}

func TestRowsToRefCustomRows(t *testing.T) {

	t.Run("transform rows into references of custom rows", func(t *testing.T) {
		peopleJson := `[
            {
                "name" : "Cosmo",
                "age" : 111
            },
            {
                "name" : "Wanda",
                "age" : 111
            }
        ]`
		var rows []map[string]interface{}
		json.Unmarshal([]byte(peopleJson), &rows)
		refCustomRows := rowsToRefCustomRows(rows)
		for i := 0; i < len(rows); i++ {
			assert.Equal(t, reflect.TypeOf(refCustomRows[i]).String(), "*bq.CustomRow", "refCustomRows[i] has wrong type")
			assert.Equal(t, refCustomRows[i].valMap, rows[i])
		}
		assert.NotEqual(t, refCustomRows[0], refCustomRows[1], "Var refCustomRows should contain different references.")
	})

}
