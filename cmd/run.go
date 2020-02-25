package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/api/googleapi"

	BQUtil "github.com/anpandu/ps2bq/internal/util/bq"
	CommonUtil "github.com/anpandu/ps2bq/internal/util/common"
	PSUtil "github.com/anpandu/ps2bq/internal/util/pubsub"
	WorkerUtil "github.com/anpandu/ps2bq/internal/util/worker"
	log "github.com/sirupsen/logrus"
)

var (
	// FLAGS
	flagProject        string
	flagDataset        string
	flagTable          string
	flagSchema         string
	flagMessageBuffer  int
	flagWorker         int
	flagTopic          string
	flagSubscriptionID string
)

func initFlagVars() {
	flagProject = viper.GetString("PROJECT")
	flagDataset = viper.GetString("DATASET")
	flagTable = viper.GetString("TABLE")
	flagSchema = viper.GetString("SCHEMA")
	flagMessageBuffer = viper.GetInt("MESSAGE_BUFFER")
	flagWorker = viper.GetInt("WORKER")
	flagTopic = viper.GetString("TOPIC")
	flagSubscriptionID = viper.GetString("SUBSCRIPTION_ID")
}

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	runCmd.PersistentFlags().StringP("project", "P", "", "Google Cloud Platform Project ID")
	runCmd.PersistentFlags().StringP("dataset", "D", "", "BigQuery Dataset")
	runCmd.PersistentFlags().StringP("table", "T", "", "BigQuery Table")
	runCmd.PersistentFlags().String("schema", "/tmp/schema.json", "BigQuery JSON table schema file location")
	runCmd.PersistentFlags().IntP("message-buffer", "n", 1, "Number of message to be inserted")
	runCmd.PersistentFlags().IntP("worker", "w", 4, "Number of workers")
	runCmd.PersistentFlags().StringP("topic", "t", "", "PubSub Topic")
	runCmd.PersistentFlags().StringP("subscription-id", "s", "", "PubSub Subscription ID")
	viper.BindEnv("PROJECT")
	viper.BindPFlag("PROJECT", runCmd.PersistentFlags().Lookup("project"))
	viper.BindEnv("DATASET")
	viper.BindPFlag("DATASET", runCmd.PersistentFlags().Lookup("dataset"))
	viper.BindEnv("TABLE")
	viper.BindPFlag("TABLE", runCmd.PersistentFlags().Lookup("table"))
	viper.BindEnv("SCHEMA")
	viper.BindPFlag("SCHEMA", runCmd.PersistentFlags().Lookup("schema"))
	viper.BindEnv("MESSAGE_BUFFER")
	viper.BindPFlag("MESSAGE_BUFFER", runCmd.PersistentFlags().Lookup("message-buffer"))
	viper.BindEnv("WORKER")
	viper.BindPFlag("WORKER", runCmd.PersistentFlags().Lookup("worker"))
	viper.BindEnv("TOPIC")
	viper.BindPFlag("TOPIC", runCmd.PersistentFlags().Lookup("topic"))
	viper.BindEnv("SUBSCRIPTION_ID")
	viper.BindPFlag("SUBSCRIPTION_ID", runCmd.PersistentFlags().Lookup("subscription-id"))
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Import messages from google Pubsub into a BigQuery table",
	Long: `Import messages from google Pubsub into a BigQuery table.
PubSub Messages received should be JSON.
BigQuery table will be created if not exist according to schema specified.
PubSub Messages will each inserted as a new row.`,
	Run: run,
}

func run(cmd *cobra.Command, args []string) {
	// f, _ := os.Create("trace.out")
	// defer f.Close()
	// _ = trace.Start(f)
	// defer trace.Stop()

	runtime.GOMAXPROCS(2)
	initFlagVars()
	rand.Seed(time.Now().UTC().UnixNano())
	defer func() func() {
		start := time.Now()
		return func() {
			log.Info("done in", time.Since(start).Seconds(), "seconds")
		}
	}()()

	log.Info(fmt.Sprintf("Creating Table \"%s.%s.%s\"\n", flagProject, flagDataset, flagTable))
	schemaJSON := CommonUtil.ReadFileToString(flagSchema)
	log.Info(schemaJSON)
	// TO-DO: Create Table with partition
	err := BQUtil.CreateTableExplicitSchema(flagProject, flagDataset, flagTable, schemaJSON)
	if err != nil {
		// TO-DO: handle table creating error
		if _, ok := err.(*googleapi.Error); ok {
			log.Info("Table already exist, not created")
		} else {
			log.Fatal(err)
		}
	} else {
		log.Info("Table created, waiting 10 seconds")
		time.Sleep(10000 * time.Millisecond)
	}

	var messageChan = make(chan interface{})
	var wg sync.WaitGroup
	log.Info(fmt.Sprintf("Deploying Workers: w=%d n=%d", flagWorker, flagMessageBuffer))
	WorkerUtil.DeployWorkers(messageChan, flagMessageBuffer, flagWorker, processMessages, &wg)
	log.Info("Start Processing")
	sendPubSubMessagesToChannel(flagProject, flagSubscriptionID, messageChan)
	// sendDummyMessage(messageChan)
	wg.Wait()
}

func sendPubSubMessagesToChannel(projectID string, subID string, messageChan chan<- interface{}) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	// TO-DO: autocreate + autogenerate sub id
	sub := client.Subscription(subID)
	cctx, _ := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		messageChan <- msg
	})
	if err != nil {
		log.Fatal(err)
	}
}

func sendDummyMessage(messageChan chan<- interface{}) {
	for i := 0; i < 64; i++ {
		message := fmt.Sprintf(`{"name":"a%d","age":%d}`, i, i)
		if rand.Int31n(8) < 1 {
			// message = fmt.Sprintf("{}broke%d", i)
			message = fmt.Sprintf(`{"name":"a%d","agex":%d}`, i, i)
		}
		messageChan <- message
		time.Sleep(50 * time.Millisecond)
		// log.Info("send", message)
	}
	log.Info("finished")
	close(messageChan)
}

func processMessages(messages []interface{}, workerID int) error {
	// FILTER MESSAGES
	var rows []map[string]interface{} // contains JSON of Message data
	var validRowsStr []string         // contains JSON string of Message data, for logging later
	var invalidRowsStr []string
	for _, message := range messages {
		msgData := message.(*pubsub.Message).Data
		row, err := PSUtil.MessageDataToJSONObject(msgData)
		// TO-DO: Schema Validation
		// TO-DO: flush when reaching n seconds
		if err == nil {
			rows = append(rows, row)
			validRowsStr = append(validRowsStr, string(msgData))
		} else {
			invalidRowsStr = append(invalidRowsStr, string(msgData))
		}
	}
	// INSERT TO BQ
	if len(rows) > 0 {
		err := BQUtil.InsertRows(flagProject, flagDataset, flagTable, rows)
		if err != nil {
			invalidRowsStr = append(invalidRowsStr, validRowsStr...)
		} else {
			log.Info(fmt.Sprintf("Worker #%d - %d message(s) inserted - %s", workerID, len(rows), validRowsStr))
		}
	}
	if len(invalidRowsStr) > 0 {
		log.Error(fmt.Sprintf("Worker #%d - %d message(s) rejected - %s", workerID, len(invalidRowsStr), invalidRowsStr))
	}
	for _, message := range messages {
		// TO-DO: lost connection must not be Ack-ed
		message.(*pubsub.Message).Ack()
	}
	return nil
}
