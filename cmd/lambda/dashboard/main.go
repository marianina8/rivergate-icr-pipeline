// Command dashboard is the hosted web UI Lambda (API Gateway HTTP API):
// submit tickets, watch them get classified by the worker, review the
// queue. Every page sits behind a shared password stored in SSM. It is
// mounted at ICR_DASHBOARD_BASE_PATH (/demos/rivergate) so marian.online can
// proxy it under its own domain.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/marianina8/rivergate-icr-pipeline/internal/awsapp"
	"github.com/marianina8/rivergate-icr-pipeline/internal/dashboard"
	"github.com/marianina8/rivergate-icr-pipeline/internal/httplambda"
)

func main() {
	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	// The dashboard only ingests and reviews; the worker Lambda classifies.
	app, err := awsapp.New(ctx, awsapp.Options{Classifier: "mock", Log: log})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}

	password := os.Getenv("ICR_DASHBOARD_PASSWORD") // sam local only
	if password == "" {
		password, err = awsapp.FetchSecret(ctx, ssm.NewFromConfig(app.AWS), os.Getenv("ICR_DASHBOARD_PASSWORD_PARAM"))
	}
	if err != nil || password == "" {
		// Fail closed: lock the UI with an unguessable password nobody knows.
		log.Error("dashboard password unavailable; the UI is locked until it is set in SSM", "err", err)
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		password = hex.EncodeToString(b)
	}

	exDir := os.Getenv("ICR_EXAMPLES_DIR")
	if exDir != "" && !filepath.IsAbs(exDir) {
		exDir = filepath.Join(os.Getenv("LAMBDA_TASK_ROOT"), exDir)
	}
	examples, err := dashboard.LoadExamples(exDir)
	if err != nil {
		log.Warn("examples not loaded", "err", err)
	}
	var origins []string
	for _, o := range strings.Split(os.Getenv("ICR_DASHBOARD_ALLOWED_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, o)
		}
	}
	s, err := dashboard.New(dashboard.Options{
		Svc: app.Svc, Actions: dashboard.AuditActions(app.Svc), Reviewer: "demo-reviewer",
		Password: password, SecureCookie: true, Examples: examples,
		BasePath: os.Getenv("ICR_DASHBOARD_BASE_PATH"), AllowedOrigins: origins, SiteURL: os.Getenv("ICR_DASHBOARD_SITE_URL"),
	})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	lambda.Start(httplambda.Handler(s.Handler()))
}
