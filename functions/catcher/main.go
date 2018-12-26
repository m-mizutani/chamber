package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

type result struct {
	Result string       `json:"result"`
	Done   int          `json:"done"`
	Errors []*errorInfo `json:"errors"`
}

type errorInfo struct {
	Error   error          `json:"error"`
	S3Event events.S3Event `json:"s3event"`
}

type argument struct {
	errorTable string
	awsRegion  string
	event      events.SNSEvent
}

type errorRecord struct {
	S3Key        string    `dynamo:"s3key"`
	OccurredAt   time.Time `dynamo:"occurred_at"`
	RequestID    string    `dynamo:"request_id"`
	ErrorMessage string    `dynamo:"error_message"`
	S3Event      []byte    `dynamo:"s3event"`
	ErrorCount   int       `dynamo:"error_count"`
}

type messageAttribute struct {
	Type  string
	Value string
}

func decodeViaJSON(src interface{}, dst interface{}) error {
	rawData, err := json.Marshal(src)
	if err != nil {
		return err
	}

	err = json.Unmarshal(rawData, dst)
	if err != nil {
		return err
	}

	return nil
}

func handleEvent(record events.SNSEventRecord, table dynamo.Table) *errorInfo {
	errInfo := &errorInfo{nil, events.S3Event{}}

	errMsgEntity, ok := record.SNS.MessageAttributes["ErrorMessage"]
	if !ok {
		logger.WithField("record", record).Warn("No ErrorMessage")
		return errInfo
	}

	var errMsg messageAttribute
	err := decodeViaJSON(errMsgEntity, &errMsg)
	if err != nil {
		logger.WithField("errMsgEntity", errMsgEntity).Warn("ErrorMessage can not be converted to MessageAttribute")
		return errInfo
	}

	s3Msg := []byte(record.SNS.Message)
	var s3event events.S3Event
	err = json.Unmarshal(s3Msg, &s3event)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"message": record.SNS.Message,
			"error":   err,
		}).Error("Fail to parse json as S3 event")
		return errInfo
	}

	if len(s3event.Records) != 1 {
		logger.WithField("event", s3event).Error("S3 record size is not 1")
		return errInfo
	}

	s3record := s3event.Records[0]
	s3Key := s3record.S3.Bucket.Name + "/" + s3record.S3.Object.Key
	rec := errorRecord{
		S3Key:        s3Key,
		OccurredAt:   record.SNS.Timestamp,
		ErrorMessage: errMsg.Value,
		ErrorCount:   1,
		S3Event:      s3Msg,
	}

	err = table.Put(rec).If("attribute_not_exists(s3key)").Run()
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				// Fail to put a new record because the record already exists
				var newRecord errorRecord
				err = table.Update("s3key", s3Key).Add("error_count", 1).Value(&newRecord)

				if err != nil {
					logger.WithFields(logrus.Fields{
						"error":  err,
						"record": rec,
					}).Error("Fail to update error count")
					return errInfo
				}

				logger.WithField("new", newRecord).Info("Updated the existing record")

				return nil
			}
		}

		// Fail to put a new record other than existing record
		logger.WithFields(logrus.Fields{
			"error":  err,
			"record": rec,
		}).Error("Fail to put error data")
		return errInfo
	}

	// Succeeded to put a new record
	logger.WithField("new", rec).Info("Inserted a new record")

	return nil
}

func handler(args argument) (result, error) {
	var res result
	logger.WithField("event", args.event).Info("Start")

	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(args.awsRegion)})
	table := db.Table(args.errorTable)

	for _, record := range args.event.Records {
		errInfo := handleEvent(record, table)
		if errInfo != nil {
			res.Errors = append(res.Errors, errInfo)
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
