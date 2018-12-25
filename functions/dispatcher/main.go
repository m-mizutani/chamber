package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	lambdaService "github.com/aws/aws-sdk-go/service/lambda"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

type result struct {
	Result string `json:"result"`
	Done   int    `json:"done"`
}

type argument struct {
	lambdaArn       string
	awsRegion       string
	whitePrefixList []string
	event           events.KinesisEvent
	ctx             context.Context
}

func matchWhiteList(s3record events.S3EventRecord, whitelist []string) bool {
	path := s3record.S3.Bucket.Name + "/" + s3record.S3.Object.Key

	for _, wprefix := range whitelist {
		if strings.HasPrefix(path, wprefix) {
			logger.WithFields(logrus.Fields{
				"prefix": wprefix,
				"s3":     s3record,
			}).Info("matched whitelist prefix, skip")

			return true
		}
	}

	return false
}

func invokeLambda(s3record events.S3EventRecord, lambdaArn string, svc *lambdaService.Lambda) error {

	ev := events.S3Event{[]events.S3EventRecord{s3record}}
	rawData, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	input := &lambdaService.InvokeInput{
		FunctionName:   aws.String(lambdaArn),
		InvocationType: aws.String("Event"),
		Payload:        rawData,
	}

	result, err := svc.Invoke(input)
	if err != nil {
		return err
	}

	logger.WithFields(logrus.Fields{
		"input":  input,
		"result": result,
	}).Info("Invoked")

	return nil
}

func handler(args argument) (result, error) {
	var res result

	logger.WithField("args", args).Info("Start function")

	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(args.awsRegion),
	}))
	svc := lambdaService.New(ssn)

	for _, record := range args.event.Records {
		var s3event events.S3Event
		err := json.Unmarshal(record.Kinesis.Data, &s3event)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
				"data":  string(record.Kinesis.Data),
			}).Warn("Fail to unmarshal s3 event")
			continue
		}

		for _, s3record := range s3event.Records {
			logger.WithField("s3record", s3record).Info("S3 record")

			if len(args.whitePrefixList) > 0 && !matchWhiteList(s3record, args.whitePrefixList) {
				continue
			}

			err := invokeLambda(s3record, args.lambdaArn, svc)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"s3record": s3record,
				}).Warn("Invoke Error")
			}
		}
	}

	return res, nil
}

func handleRequest(ctx context.Context, event events.KinesisEvent) (result, error) {
	logger.WithField("event", event).Info("Start")

	args := argument{
		lambdaArn:       os.Getenv("TARGET_LAMBDA_ARN"),
		awsRegion:       os.Getenv("AWS_REGION"),
		whitePrefixList: strings.Split(os.Getenv("WHITE_PREFIX_LIST"), ","),
		event:           event,
		ctx:             ctx,
	}

	return handler(args)
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	lambda.Start(handleRequest)
}
