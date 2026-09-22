// Command worker is the SQS-triggered Lambda: normalize -> classify (Bedrock)
// -> write state (DynamoDB) -> route. Built by `sam build` via the Makefile
// target build-WorkerFunction.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/marianina8/rivergate-icr-pipeline/internal/awsapp"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	app, err := awsapp.New(context.Background(), awsapp.Options{Log: log})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	w := &awsapp.Worker{Svc: app.Svc, S3: s3.NewFromConfig(app.AWS), Log: log}
	lambda.Start(w.Handle)
}
