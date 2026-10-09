#!/bin/bash
set -euo pipefail

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_ARN="$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"
REDRIVE="$(printf '{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":\"5\"}' "$DLQ_ARN")"
REDRIVE_ESCAPED="$(printf '%s' "$REDRIVE" | sed 's/\\/\\\\/g; s/"/\\"/g')"
TRANSACTION_ATTRIBUTES="$(printf '{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"60\",\"RedrivePolicy\":\"%s\"}' "$REDRIVE_ESCAPED")"

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$TRANSACTION_ATTRIBUTES"

awslocal sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=60
