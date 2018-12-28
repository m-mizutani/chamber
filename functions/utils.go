package functions

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"

	"github.com/sirupsen/logrus"
)

type LambdaInvoker struct {
	svc       *lambda.Lambda
	lambdaArn string
}

func NewLambdaInvoker(region, lambdaArn string) LambdaInvoker {
	invoker := LambdaInvoker{
		lambdaArn: lambdaArn,
	}

	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	invoker.svc = lambda.New(ssn)

	return invoker
}

func (x *LambdaInvoker) Invoke(s3record events.S3EventRecord) error {
	ev := events.S3Event{[]events.S3EventRecord{s3record}}
	rawData, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	input := &lambda.InvokeInput{
		FunctionName:   aws.String(x.lambdaArn),
		InvocationType: aws.String("Event"),
		Payload:        rawData,
	}

	_, err = x.svc.Invoke(input)
	if err != nil {
		return err
	}

	return nil
}

func NewLogger() *logrus.Entry {
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	baseLogger.SetFormatter(&logrus.JSONFormatter{})

	return baseLogger.WithFields(logrus.Fields{})

}

func SetLoggerContext(logger *logrus.Entry, ctx context.Context) *logrus.Entry {
	var requestID string

	if lc, ok := lambdacontext.FromContext(ctx); ok {
		requestID = lc.AwsRequestID
	} else {
		logger.WithField("context", ctx).Warn("Fail to extract lambda context")
		requestID = "N/A"
	}

	return logger.WithField("lambda_request_id", requestID)
}
