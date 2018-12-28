package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/m-mizutani/chamber/functions"
)

var logger = functions.NewLogger()

// argment is a parameters to invoke Catcher
type argument struct {
	LambdaArn string
	MaxRetry  string
	AwsRegion string
	Event     events.DynamoDBEvent
}

// result is a returned value of Catcher Lambda function.
type result struct {
	Result string      `json:"result"`
	Errors []errorInfo `json:"errors"`
}

type errorInfo struct {
	S3Key string
	Error error
}

func handleRecord(dynamoRecord events.DynamoDBEventRecord, invoker functions.LambdaInvoker, maxRetry uint64) (string, error) {
	var s3key string

	// Setup dynamoDB accessor
	tableArnSeq := strings.Split(dynamoRecord.EventSourceArn, "/")
	if len(tableArnSeq) != 4 {
		logger.WithField("eventSourceArn", dynamoRecord.EventSourceArn).
			Error("Invalid EventSourceArn format")
		return s3key, errors.New("Invalid EventSourceArn format")
	}

	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(dynamoRecord.AWSRegion)})
	table := db.Table(tableArnSeq[1])

	// Retrieve S3 key
	if newKey, ok := dynamoRecord.Change.NewImage["s3key"]; ok {
		s3key = newKey.String()
	} else {
		return s3key, errors.New("Fail to get s3key from Dynamodb Record")
	}

	// Retrieve error count.
	if newCount, ok := dynamoRecord.Change.NewImage["error_count"]; ok {
		count, err := newCount.Integer()
		if err != nil {
			return s3key, errors.Wrap(err, "Fail to get error count")
		}

		if uint64(count) > maxRetry {
			logger.WithFields(logrus.Fields{
				"count":    count,
				"maxRetry": maxRetry,
				"s3key":    s3key,
			}).Info("Skip retrying for S3 key")

			return s3key, nil
		}
	} else {
		return s3key, errors.New("Fail to get s3event from Dynamodb Record")
	}

	if newEvent, ok := dynamoRecord.Change.NewImage["s3event"]; ok {
		var s3event events.S3Event
		err := json.Unmarshal(newEvent.Binary(), &s3event)
		if err != nil {
			return s3key, errors.Wrap(err, "Fail to parse s3event in dynamoDB record")
		}

		if len(s3event.Records) != 1 {
			return s3key, errors.New("Invalid S3 record set length, must be 1")
		}

		s3record := s3event.Records[0]

		logger.WithFields(logrus.Fields{
			"s3record": s3record,
		}).Info("Invoking lambda")

		// Lock if can
		err = table.Update("s3key", s3key).Set("retried", true).
			If("retried = ?", false).Run()
		if err != nil {
			return s3key, errors.Wrap(err, "Fail to update target record")
		}

		err = invoker.Invoke(s3record)
		if err != nil {
			return s3key, errors.Wrap(err, "Fail to invoke Lambda")
		}
	}

	return s3key, nil
}

func handler(args argument) (result, error) {
	var res result

	logger.WithFields(logrus.Fields{
		"args": args,
	}).Info("Start function")

	invoker := functions.NewLambdaInvoker(args.AwsRegion, args.LambdaArn)
	maxRetry, err := strconv.ParseUint(args.MaxRetry, 10, 64)
	if err != nil {
		return res, errors.Wrapf(err, "Fail to parse MaxRetry: '%s'", args.MaxRetry)
	}

	for _, dynamoRecord := range args.Event.Records {
		s3key, err := handleRecord(dynamoRecord, invoker, maxRetry)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"dynamodb_record": dynamoRecord,
				"s3key":           s3key,
				"error":           err,
			}).Error("Fail to handle dynamodb record")

			res.Errors = append(res.Errors, errorInfo{s3key, err})
		}
	}

	return res, nil
}

func main() {
	lambda.Start(func(ctx context.Context, event events.DynamoDBEvent) (result, error) {
		args := argument{
			LambdaArn: os.Getenv("TARGET_LAMBDA_ARN"),
			MaxRetry:  os.Getenv("MAX_RETRY"),
			AwsRegion: os.Getenv("AWS_REGION"),
			Event:     event,
		}

		return handler(args)
	})
}
