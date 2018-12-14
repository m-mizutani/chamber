package main

import (
	"context"
	"os"

	"github.com/guregu/dynamo"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

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
}

func handler(args argument) (result, error) {
	var res result

	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(args.awsRegion)})
	table := db.Table(args.errorTable)

	for _, record := range args.event.Records {
		errMsgEntity, ok := record.SNS.MessageAttributes["ErrorMessage"]
		if !ok {
			log.WithField("record", record).Warn("No ErrorMessage")
			continue
		}
		errMsg, ok := errMsgEntity.(string)
		if !ok {
			log.WithField("record", record).Warn("ErrorMessage is not string")
			continue
		}

		rec := errorRecord{
			S3Key:        "tmp",
			ErrorMessage: errMsg,
		}
		err := table.Put(rec).Run()
		if err != nil {
			log.WithField("error", err).Warn("Fail to put error data")
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
	lambda.Start(handleRequest)
}
