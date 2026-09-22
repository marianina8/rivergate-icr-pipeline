// Command webhook is the API Gateway (HTTP API) Lambda behind POST /tickets.
// Callers must send the shared secret in the X-Rivergate-Token header.
// Built by `sam build` via the Makefile target build-WebhookFunction.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/marianina8/rivergate-icr-pipeline/internal/awsapp"
)

func main() {
	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	app, err := awsapp.New(ctx, awsapp.Options{Log: log})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	// ICR_WEBHOOK_TOKEN is for `sam local` only; deployed stacks read SSM.
	token := os.Getenv("ICR_WEBHOOK_TOKEN")
	if token == "" {
		token, err = awsapp.FetchSecret(ctx, ssm.NewFromConfig(app.AWS), os.Getenv("ICR_WEBHOOK_SECRET_PARAM"))
		if err != nil {
			// Fail closed: the handler rejects every request without a secret.
			log.Error("webhook secret unavailable; all requests will be rejected", "err", err)
		}
	}
	h := &awsapp.Webhook{Svc: app.Svc, Token: token, Log: log}
	lambda.Start(h.Handle)
}
