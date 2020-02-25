# ps2bq &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/anpandu/ps2bq/blob/master/LICENSE) 

## Introduction

PS2BQ is a CLI tool for importing messages from GCP PubSub into BigQuery table.

## Usage

### Run from local

```sh
# clone repo
git clone https://github.com/anpandu/ps2bq

# build binary
go mod download
go install

# run it
export GOOGLE_APPLICATION_CREDENTIALS=~/google-key.json # make sure credential file is set
$GOPATH/bin/ps2bq run --help

# EXAMPLE
ps2bq run
    --project=myproject
    --dataset=mydataset
    --table=students
    --topic=t-students
    --subscription-id=ps2bq-students-20200101
    --worker=4
    --message-buffer=1
```

### Run as Docker container

```sh
# TBD
```

### Configs

```
  -D, --dataset string           BigQuery Dataset
  -n, --message-buffer int       Number of message to be inserted (default 1)
  -P, --project string           Google Cloud Platform Project ID
      --schema string            BigQuery JSON table schema file location (default "/tmp/schema.json")
  -s, --subscription-id string   PubSub Subscription ID
  -T, --table string             BigQuery Table
  -t, --topic string             PubSub Topic
  -w, --worker int               Number of workers (default 4)
```

PubSub Messages will each inserted as a new row.
PubSub Messages received should be JSON Object.
Message example: `{"id":123,"name":"Alice"}`
JSON Schema must be provided in order to create table.
See: https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file
Message containing invalid JSON and/or invalid according to table schema will be failed to be inserted.

## Roadmap
| Status  | Description |
|:-------:|:----------- |
|    ✔    | 1 worker, 1 message inserted |
|    ✔    | N worker, 1 message inserted each |
|    ✔    | N worker, N message inserted each (buffered) |
|    ✘    | Every t seconds, insert all messages in buffer |
|    ✘    | Validate message using JSON schema |
|    ✘    | Create table with partition |
|    ✘    | Auto-generate subscription ID |
|    ✘    | go doc |
|    ✘    | Multiple sink (?) |
|    ✘    | Multiple source (?) |


## License

MIT © [Ananta Pandu](anpandumail@gmail.com)