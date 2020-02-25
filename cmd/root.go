package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "ps2bq",
		Short: "A tool for importing data from google Pubsub to BigQuery.",
		Long: `PS2BQ is a CLI tool for importing messages from GCP PubSub into BigQuery table.
It will continously running as a foreground process.
PubSub Message received should be JSON object.`,
		Version: "1.0.0",
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func er(msg interface{}) {
	fmt.Println("Error:", msg)
	os.Exit(1)
}
