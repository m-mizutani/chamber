#!/bin/bash

if [ $# -le 1 ]; then
    echo "usage) $0 StackName ConfigFilePath"
    exit 1
fi

TEMPLATE_FILE=template.yml
OUTPUT_FILE=`mktemp`

echo "SAM: $OUTPUT_FILE"

CODE_S3_BUCKET=`cat $2 | jq '.["CodeS3Bucket"]' -r`
CODE_S3_PREFIX=`cat $2 | jq '.["CodeS3Prefix"]' -r`

aws cloudformation package --template-file $TEMPLATE_FILE \
    --output-template-file $OUTPUT_FILE \
    --s3-bucket $CODE_S3_BUCKET --s3-prefix $CODE_S3_PREFIX
aws cloudformation deploy --template-file $OUTPUT_FILE --stack-name $1 \
    --capabilities CAPABILITY_IAM

Resources=`aws cloudformation describe-stack-resources --stack-name $1 | jq '.StackResources[]'`
LambdaArn=`echo $Resources | jq 'select(.LogicalResourceId == "Tester") | .PhysicalResourceId' -r`
DlqSnsArn=`echo $Resources | jq 'select(.LogicalResourceId == "DLQ") | .PhysicalResourceId' -r`


cat <<EOF > param.json
{
  "LambdaArn": "$LambdaArn",
  "DlqSnsArn": "$DlqSnsArn"
}
EOF

echo ""
echo "done"
