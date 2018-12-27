package main_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/google/uuid"
	"github.com/guregu/dynamo"
	"github.com/m-mizutani/generalprobe"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parameter struct {
	StackName        string
	AwsRegion        string `json:"Region"`
	KinesisStreamArn string
	LambdaArn        string
	DlqSnsArn        string
}

func newParameter() parameter {
	p := parameter{}

	cfgPath := os.Getenv("CHAMBER_CONFIG")
	fp, err := os.Open(cfgPath)
	if err != nil {
		log.WithFields(log.Fields{
			"error":      err,
			"configPath": cfgPath,
		}).Fatal("Fail to open config file")
	}

	jdata, err := ioutil.ReadAll(fp)
	if err != nil {
		log.WithField("error", err).Fatal("Fail to read config")
	}

	err = json.Unmarshal(jdata, &p)
	if err != nil {
		log.WithField("error", err).Fatal("Fail to unmarshal config")
	}

	return p
}

func TestMain(t *testing.T) {
	param := newParameter()
	id := uuid.New().String()

	var ev events.S3Event
	ev.Records = []events.S3EventRecord{
		events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: id},
			},
		},
	}
	rawData, err := json.Marshal(ev)
	require.NoError(t, err)

	g := generalprobe.New(param.AwsRegion, param.StackName)
	g.AddScenes([]generalprobe.Scene{
		g.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),
	})
	g.Run()
}

func TestWhiteList1(t *testing.T) {
	param := newParameter()
	id := uuid.New().String()

	var ev events.S3Event
	ev.Records = []events.S3EventRecord{
		events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "whitelist"},
				Object: events.S3Object{Key: "test1/" + id},
			},
		},
	}
	rawData, err := json.Marshal(ev)
	require.NoError(t, err)

	g := generalprobe.New(param.AwsRegion, param.StackName)
	g.AddScenes([]generalprobe.Scene{
		g.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),

		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),
	})
	g.Run()
}

func TestWhiteList2(t *testing.T) {
	param := newParameter()
	id := uuid.New().String()

	var ev events.S3Event
	ev.Records = []events.S3EventRecord{
		events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "whitelist"},
				Object: events.S3Object{Key: id},
			},
		},
	}
	rawData, err := json.Marshal(ev)
	require.NoError(t, err)

	g := generalprobe.New(param.AwsRegion, param.StackName)
	g.AddScenes([]generalprobe.Scene{
		g.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 0, len(logs))
			return true
		}),
	})
	g.Run()
}

type errorTableRecord struct {
	S3Key        string    `dynamo:"s3key"`
	OccurredAt   time.Time `dynamo:"occurred_at"`
	RequestID    string    `dynamo:"request_id"`
	ErrorMessage string    `dynamo:"error_message"`
	S3Event      []byte    `dynamo:"s3event"`
	ErrorCount   int       `dynamo:"error_count"`
	Retried      bool      `dynamo:"retried"`
}

func TestFireDLQ(t *testing.T) {
	if os.Getenv("CHAMBER_SKIP_DLQ_TEST") != "" {
		t.Skip()
	}

	param := newParameter()
	id := uuid.New().String()

	bucketName := "test-bucket"
	var ev events.S3Event
	ev.Records = []events.S3EventRecord{
		events.S3EventRecord{
			EventName: "error",
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: bucketName},
				Object: events.S3Object{Key: id},
			},
		},
	}
	rawData, err := json.Marshal(ev)
	require.NoError(t, err)

	g := generalprobe.New(param.AwsRegion, param.StackName)
	g.AddScenes([]generalprobe.Scene{
		g.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),

		g.GetLambdaLogs(g.LogicalID("Catcher"), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}).SetInterval(15).SetQueryLimit(30),

		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}).SetInterval(15).SetQueryLimit(30),

		g.GetDynamoRecord(g.LogicalID("ErrorTable"), func(table dynamo.Table) bool {
			var errRecord errorTableRecord
			key := bucketName + "/" + id

			err := table.Get("s3key", key).One(&errRecord)
			assert.NoError(t, err)
			assert.Equal(t, "Test Error", errRecord.ErrorMessage)

			return true
		}),
	})
	g.Run()
}

func TestCatcher(t *testing.T) {
	param := newParameter()
	id := uuid.New().String()

	bucketName := "test-bucket"
	var s3Event events.S3Event
	s3Event.Records = []events.S3EventRecord{
		events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: bucketName},
				Object: events.S3Object{Key: id},
			},
		},
	}

	s3Msg, err := json.Marshal(s3Event)
	require.NoError(t, err)

	attr := sns.MessageAttributeValue{}
	attr.SetDataType("String")
	attr.SetStringValue("Blue")
	snsAttrs := map[string]*sns.MessageAttributeValue{
		"ErrorMessage": &attr,
	}

	g := generalprobe.New(param.AwsRegion, param.StackName)
	// g.SetLoggerDebugLevel()
	g.AddScenes([]generalprobe.Scene{
		g.PublishSnsMessage(g.Arn(param.DlqSnsArn), s3Msg).AddMessageAttributes(snsAttrs),

		g.GetLambdaLogs(g.LogicalID("Catcher"), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),

		g.GetDynamoRecord(g.LogicalID("ErrorTable"), func(table dynamo.Table) bool {
			var errRecord errorTableRecord
			key := bucketName + "/" + id

			err := table.Get("s3key", key).One(&errRecord)
			assert.NoError(t, err)
			assert.Equal(t, "Blue", errRecord.ErrorMessage)

			return true
		}),

		// Reloader recieves DynamoDB table change record
		g.GetLambdaLogs(g.LogicalID("Reloader"), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),
	})

	g.Run()
}

func TestCountUp(t *testing.T) {
	param := newParameter()
	id := uuid.New().String()

	bucketName := "test-bucket"
	var s3Event events.S3Event
	s3Event.Records = []events.S3EventRecord{
		events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: bucketName},
				Object: events.S3Object{Key: id},
			},
		},
	}

	s3Msg, err := json.Marshal(s3Event)
	require.NoError(t, err)

	attr := sns.MessageAttributeValue{}
	attr.SetDataType("String")
	attr.SetStringValue("Blue")
	snsAttrs := map[string]*sns.MessageAttributeValue{
		"ErrorMessage": &attr,
	}

	g := generalprobe.New(param.AwsRegion, param.StackName)
	// g.SetLoggerDebugLevel()
	g.AddScenes([]generalprobe.Scene{
		g.PublishSnsMessage(g.Arn(param.DlqSnsArn), s3Msg).AddMessageAttributes(snsAttrs),

		g.GetDynamoRecord(g.LogicalID("ErrorTable"), func(table dynamo.Table) bool {
			var errRecord errorTableRecord
			key := bucketName + "/" + id

			err := table.Get("s3key", key).One(&errRecord)
			assert.NoError(t, err)
			if 1 != errRecord.ErrorCount {
				return false
			}

			return true
		}),

		g.GetLambdaLogs(g.LogicalID("Reloader"), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),

		// Target Lambda should be invoked for first time.
		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}),

		// Send second (dummy) DLQ message, then error_count should be count up.
		g.PublishSnsMessage(g.Arn(param.DlqSnsArn), s3Msg).AddMessageAttributes(snsAttrs),

		g.GetDynamoRecord(g.LogicalID("ErrorTable"), func(table dynamo.Table) bool {
			var errRecord errorTableRecord
			key := bucketName + "/" + id

			err := table.Get("s3key", key).One(&errRecord)
			assert.NoError(t, err)
			if 2 != errRecord.ErrorCount {
				return false
			}

			return true
		}),

		g.Pause(30),

		// Target Lambda should NOT be invoked for the second time.
		g.GetLambdaLogs(g.Arn(param.LambdaArn), id, func(logs []string) bool {
			assert.Equal(t, 1, len(logs))
			return true
		}).SetQueryLimit(10),
	})

	g.Run()
}
