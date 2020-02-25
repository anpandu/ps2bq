package bq

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

type CustomRow struct {
	valMap map[string]interface{}
}

// Save implements the ValueSaver interface.
func (i *CustomRow) Save() (map[string]bigquery.Value, string, error) {
	result := make(map[string]bigquery.Value)
	for k, v := range i.valMap {
		result[k] = v
	}
	return result, "", nil
}

func rowsToRefCustomRows(rows []map[string]interface{}) []*CustomRow {
	// Transform to *CustomRow
	var refCustomRows []*CustomRow
	for i := 0; i < len(rows); i++ {
		e := CustomRow{valMap: rows[i]}
		refCustomRows = append(refCustomRows, &e)
	}
	return refCustomRows
}

// Create a table using BQ JSON schema definition
func CreateTableExplicitSchema(projectID string, datasetID string, tableID string, schemaJSON string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	schema, err := bigquery.SchemaFromJSON([]byte(schemaJSON))
	if err != nil {
		return fmt.Errorf("bigquery.SchemaFromJSON: %v", err)
	}
	metaData := &bigquery.TableMetadata{
		Schema: schema,
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metaData); err != nil {
		return err
	}
	client.Close()
	return nil
}

// Inserting data into a table using the streaming insert mechanism.
func InsertRows(projectID string, datasetID string, tableID string, rows []map[string]interface{}) error {

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	// Transform to *CustomRow
	refCustomRows := rowsToRefCustomRows(rows)

	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	if err := inserter.Put(ctx, refCustomRows); err != nil {
		return err
	}
	client.Close()
	return nil
}
