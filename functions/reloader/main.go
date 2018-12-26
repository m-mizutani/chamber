package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

// argment is a parameters to invoke Catcher
type argument struct {
	LambdaArn string
	MaxRetry  string
	AwsRegion string
	Event     events.DynamoDBEvent
}

// result is a returned value of Catcher Lambda function.
type result struct {
	Result string `json:"result"`
}

func handler(args argument) (result, error) {
	var res result
	return res, nil
}

func handleRequest(ctx context.Context, event events.DynamoDBEvent) (result, error) {
	args := argument{
		LambdaArn: os.Getenv("TARGET_LAMBDA_ARN"),
		MaxRetry:  os.Getenv("MAX_RETRY"),
		AwsRegion: os.Getenv("AWS_REGION"),
	}

	logger.WithFields(logrus.Fields{
		"args":  args,
		"event": event,
	}).Info("Start function")

	return handler(args)
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	lambda.Start(handleRequest)
}
