package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/guregu/dynamo"
	"github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var logger = logrus.New()

type result struct {
	Result string `json:"result"`
	Done   int    `json:"done"`
}

type argument struct {
	errorTable string
	awsRegion  string
	event      events.SNSEvent
}

type errorRecord struct {
	S3Key        string `dynamo:"s3key"`
	RequestID    string `dynamo:"request_id"`
	ErrorMessage string `dynamo:"error_message"`
	S3Event      string `dynamo:"s3event"`
}

type messageAttribute struct {
	Type  string
	Value string
}

func handler(args argument) (result, error) {
	var res result
	logger.WithField("event", args.event).Info("Start")

	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(args.awsRegion)})
	table := db.Table(args.errorTable)

	for _, record := range args.event.Records {
		errMsgEntity, ok := record.SNS.MessageAttributes["ErrorMessage"]
		if !ok {
			logger.WithField("record", record).Warn("No ErrorMessage")
			continue
		}

		errMsg, ok := errMsgEntity.(messageAttribute)
		if !ok {
			logger.WithField("errMsgEntity", errMsgEntity).Warn("ErrorMessage is not MessageAttribute")
			continue
		}

		var s3event events.S3Event
		err := json.Unmarshal([]byte(record.SNS.Message), &s3event)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"message": record.SNS.Message,
				"error":   err,
			}).Error("Fail to parse json as S3 event")
			continue
		}

		if len(s3event.Records) != 1 {
			logger.WithField("event", s3event).Error("S3 record size is not 1")
			continue
		}

		record := s3event.Records[0]
		rec := errorRecord{
			S3Key:        record.S3.Bucket.Name + "/" + record.S3.Object.Key,
			ErrorMessage: errMsg.Value,
		}

		err = table.Put(rec).Run()
		if err != nil {
			logger.WithField("error", err).Warn("Fail to put error data")
		}
	}

	return res, nil
}

func handleRequest(ctx context.Context, event events.SNSEvent) (result, error) {
	args := argument{
		errorTable: os.Getenv("ERROR_TABLE"),
		awsRegion:  os.Getenv("AWS_REGION"),
		event:      event,
	}

	return handler(args)
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	lambda.Start(handleRequest)
}
