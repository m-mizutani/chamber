package main_test

import (
	"encoding/json"
	"fmt"
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
	StackName        string `json:"StackName"`
	AwsRegion        string `json:"Region"`
	KinesisStreamArn string `json:"KinesisStreamArn"`
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
		gp.PutKinesisStreamRecord(g.LogicalID("ResultStream"), rawData),
		gp.AdLib(func() {
			logs := g.SearchLambdaLogs(g.LogicalID("Tester"), id)
			fmt.Println(logs)
			assert.NotEqual(t, 0, len(logs))
		}),
	})
	g.Act()
}
