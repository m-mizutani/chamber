package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/pkg/errors"


	"github.com/m-mizutani/chamber/functions"
)

var logger = functions.NewLogger()

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
			}).Info("matched whitelist prefix")

			return true
		}
	}

	return false
}

func handler(args argument) (result, error) {
	var res result

	logger.WithField("args", args).Info("Start function")

	invoker := functions.NewLambdaInvoker(args.awsRegion, args.lambdaArn)

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

			if args.whitePrefixList[0] != "" && !matchWhiteList(s3record, args.whitePrefixList) {
				continue
			}

			err := invoker.Invoke(s3record)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"error":    err,
					"s3record": s3record,
				}).Error("Invoke Error")

				return res, errors.Wrap(err, "Fail to invoke Lambda")
			}

			res.Done++
		}
	}

	return res, nil
}

func main() {
	lambda.Start(func (ctx context.Context, event events.KinesisEvent) (result, error) {
		logger = functions.SetLoggerContext(logger, ctx)
		logger.WithField("event", event).Info("Start")

		args := argument{
			lambdaArn:       os.Getenv("TARGET_LAMBDA_ARN"),
			awsRegion:       os.Getenv("AWS_REGION"),
			whitePrefixList: strings.Split(os.Getenv("WHITE_PREFIX_LIST"), ","),
			event:           event,
			ctx:             ctx,
		}

		return handler(args)

	)
}
