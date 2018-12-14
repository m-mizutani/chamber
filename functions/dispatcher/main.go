package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	lambdaService "github.com/aws/aws-sdk-go/service/lambda"
	log "github.com/sirupsen/logrus"
)

type result struct {
	Result string `json:"result"`
	Done   int    `json:"done"`
}

type argument struct {
	lambdaArn string
	awsRegion string
	event     events.KinesisEvent
	ctx       context.Context
}

func handler(args argument) (result, error) {
	var res result

	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(args.awsRegion),
	}))
	svc := lambdaService.New(ssn)

	for _, record := range args.event.Records {
		input := &lambdaService.InvokeInput{
			FunctionName:   aws.String(args.lambdaArn),
			InvocationType: aws.String("Event"),
			Payload:        record.Kinesis.Data,
		}

		result, err := svc.Invoke(input)
		log.WithField("result", result).Info("Invoked")
		if err != nil {
			log.WithField("error", err).Warn("Invoke Error")
		}
	}

	return res, nil
}

func handleRequest(ctx context.Context, event events.KinesisEvent) (result, error) {
	log.WithField("event", event).Info("Start")

	args := argument{
		lambdaArn: os.Getenv("TARGET_LAMBDA_ARN"),
		awsRegion: os.Getenv("AWS_REGION"),
		event:     event,
		ctx:       ctx,
	}

	return handler(args)
}

func main() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.JSONFormatter{})

	lambda.Start(handleRequest)
}
