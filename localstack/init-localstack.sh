#!/bin/bash
set -e

ENDPOINT="http://localhost:4566"
BUCKET="ebook"
REGION="us-east-1"
LOCALSTACK_DIR="/etc/localstack/init/ready.d"

aws --endpoint-url="$ENDPOINT" s3 mb "s3://$BUCKET" --region "$REGION"

aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_1.csv" "s3://$BUCKET/stock_1.csv" --region "$REGION"
aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_2.csv" "s3://$BUCKET/stock_2.csv" --region "$REGION"
aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_3.csv" "s3://$BUCKET/stock_3.csv" --region "$REGION"
aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_4.csv" "s3://$BUCKET/stock_4.csv" --region "$REGION"
aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_5.csv" "s3://$BUCKET/stock_5.csv" --region "$REGION"
aws --endpoint-url="$ENDPOINT" s3 cp "$LOCALSTACK_DIR/stock_6.csv" "s3://$BUCKET/stock_6.csv" --region "$REGION"
