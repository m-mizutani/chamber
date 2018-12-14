CONFIG_FILE ?= "param.json"

STACK_NAME := $(shell cat $(CONFIG_FILE) | jq '.["StackName"]' -r)
CODE_S3_BUCKET := $(shell cat $(CONFIG_FILE) | jq '.["CodeS3Bucket"]' -r)
CODE_S3_PREFIX := $(shell cat $(CONFIG_FILE) | jq '.["CodeS3Prefix"]' -r)

LAMBDA_ROLE_ARN    := $(shell cat $(CONFIG_FILE) | jq '.["LambdaRoleArn"]' -r)
LAMBDA_ARN         := $(shell cat $(CONFIG_FILE) | jq '.["LambdaArn"]' -r)
DLQ_SNS_ARN        := $(shell cat $(CONFIG_FILE) | jq '.["DlqSnsArn"]' -r)
KINESIS_STREAM_ARN := $(shell cat $(CONFIG_FILE) | jq '.["KinesisStreamArn"]' -r)


PARAMETERS=LambdaRoleArn=$(LAMBDA_ROLE_ARN) LambdaArn=$(LAMBDA_ARN) DlqSnsArn=$(DLQ_SNS_ARN) KinesisStreamArn=$(KINESIS_STREAM_ARN)
TEMPLATE_FILE=template.yml
FUNCTIONS=build/dispatcher build/catcher build/reloader

all: cli

build/dispatcher: ./functions/dispatcher/*.go
	env GOARCH=amd64 GOOS=linux go build -o build/dispatcher ./functions/dispatcher/

build/catcher: ./functions/catcher/*.go
	env GOARCH=amd64 GOOS=linux go build -o build/catcher ./functions/catcher/

build/reloader: ./functions/reloader/*.go
	env GOARCH=amd64 GOOS=linux go build -o build/reloader ./functions/reloader/

test:
	go test -v ./functions/dispatcher/
	go test -v ./functions/catcher/
	go test -v ./functions/reloader/

build: $(FUNCTIONS)

sam.yml: build template.yml
	aws cloudformation package \
		--template-file $(TEMPLATE_FILE) \
		--s3-bucket $(CODE_S3_BUCKET) \
		--s3-prefix $(CODE_S3_PREFIX) \
		--output-template-file sam.yml

deploy: sam.yml
	aws cloudformation deploy \
		--template-file sam.yml \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides $(PARAMETERS)
