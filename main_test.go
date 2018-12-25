package main_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gp "github.com/m-mizutani/generalprobe"
	log "github.com/sirupsen/logrus"
)

type parameter struct {
	StackName        string
	AwsRegion        string `json:"Region"`
	KinesisStreamArn string
	LambdaArn        string
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
	log.WithField("param", param).Info("start")
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

	g := gp.New(param.AwsRegion, param.StackName)
	g.AddScenes([]gp.Scene{
		gp.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		gp.AdLib(func() {
			logs := g.SearchLambdaLogs(g.Arn(param.LambdaArn), id)
			assert.NotEqual(t, 0, len(logs))
		}),
	})
	g.Act()
}

func TestWhiteList1(t *testing.T) {
	param := newParameter()
	log.WithField("param", param).Info("start")
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

	g := gp.New(param.AwsRegion, param.StackName)
	g.AddScenes([]gp.Scene{
		gp.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		gp.AdLib(func() {
			logs := g.SearchLambdaLogs(g.Arn(param.LambdaArn), id)
			assert.NotEqual(t, 0, len(logs))
		}),
	})
	g.Act()
}

func TestWhiteList2(t *testing.T) {
	param := newParameter()
	log.WithField("param", param).Info("start")
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

	g := gp.New(param.AwsRegion, param.StackName)
	g.AddScenes([]gp.Scene{
		gp.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		gp.AdLib(func() {
			logs := g.SearchLambdaLogs(g.Arn(param.LambdaArn), id)
			assert.Equal(t, 0, len(logs))
		}),
	})
	g.Act()
}

func TestError(t *testing.T) {
	param := newParameter()
	log.WithField("param", param).Info("start")
	id := uuid.New().String()

	var ev events.S3Event
	ev.Records = []events.S3EventRecord{
		events.S3EventRecord{
			EventName: "error",
			S3: events.S3Entity{
				Bucket: events.S3Bucket{Name: "test-bucket"},
				Object: events.S3Object{Key: id},
			},
		},
	}
	rawData, err := json.Marshal(ev)
	require.NoError(t, err)

	g := gp.New(param.AwsRegion, param.StackName)
	g.AddScenes([]gp.Scene{
		gp.PutKinesisStreamRecord(g.Arn(param.KinesisStreamArn), rawData),
		gp.AdLib(func() {
			logs1 := g.SearchLambdaLogs(g.LogicalID("Catcher"), "")
			assert.NotEqual(t, 0, len(logs1))

			logs2 := g.SearchLambdaLogs(g.Arn(param.LambdaArn), id)
			assert.NotEqual(t, 0, len(logs2)) // Not invoked
		}),
	})
	g.Act()
}
