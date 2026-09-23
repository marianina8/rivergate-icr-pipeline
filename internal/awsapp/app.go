// Package awsapp wires the pipeline to real AWS services (phase 5):
// DynamoDB for state, SQS for the queue, Bedrock for classification, and
// CloudWatch Logs (via LogSink) for stubbed actions. It also holds the Lambda
// handlers, written as plain methods so they are unit-tested with synthetic
// events and fake clients — no AWS calls in tests.
package awsapp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// Options selects the AWS resources to use. Empty fields fall back to the
// environment variables the SAM template sets.
type Options struct {
	ConfigPath string // ICR_CONFIG
	Table      string // ICR_ITEMS_TABLE
	QueueURL   string // ICR_QUEUE_URL (optional: only producers need it)
	Classifier string // ICR_CLASSIFIER: bedrock | mock
	Profile    string // AWS_PROFILE, for running locally against the stack
	Region     string // AWS_REGION / config default
	ActionLog  io.Writer
	Log        *slog.Logger
}

// App is a pipeline service backed by AWS.
type App struct {
	Svc *pipeline.Service
	AWS aws.Config
	Cfg *config.Config
}

// ResolveConfigPath makes a relative ICR_CONFIG path work inside Lambda,
// where the artifact is unpacked under LAMBDA_TASK_ROOT.
func ResolveConfigPath(p string) string {
	if p == "" {
		p = pipeline.DefaultConfig
	}
	if !filepath.IsAbs(p) {
		if root := os.Getenv("LAMBDA_TASK_ROOT"); root != "" {
			return filepath.Join(root, p)
		}
	}
	return p
}

func (o *Options) fill() {
	env := func(dst *string, key string) {
		if *dst == "" {
			*dst = os.Getenv(key)
		}
	}
	env(&o.ConfigPath, "ICR_CONFIG")
	env(&o.Table, "ICR_ITEMS_TABLE")
	env(&o.QueueURL, "ICR_QUEUE_URL")
	env(&o.Classifier, "ICR_CLASSIFIER")
	if o.ActionLog == nil {
		o.ActionLog = os.Stdout
	}
}

// LoadConfig loads the instance config and applies deployment overrides
// (the template passes the Bedrock model ID; Lambda sets AWS_REGION).
func LoadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(ResolveConfigPath(path))
	if err != nil {
		return nil, err
	}
	if m := os.Getenv("ICR_BEDROCK_MODEL_ID"); m != "" {
		cfg.Classifier.Bedrock.ModelID = m
	}
	if r := os.Getenv("AWS_REGION"); r != "" {
		cfg.Classifier.Bedrock.Region = r
	}
	return cfg, nil
}

// New builds an AWS-backed pipeline. Loading AWS config makes no network
// call; the first store/queue/model call does.
func New(ctx context.Context, o Options) (*App, error) {
	o.fill()
	if o.Table == "" {
		return nil, fmt.Errorf("no DynamoDB table: set ICR_ITEMS_TABLE (the ItemsTableName stack output)")
	}
	cfg, err := LoadConfig(o.ConfigPath)
	if err != nil {
		return nil, err
	}
	// Region precedence: explicit option > AWS_REGION (always set in Lambda)
	// > the region in the instance config.
	var opts []func(*awsconfig.LoadOptions) error
	switch {
	case o.Region != "":
		opts = append(opts, awsconfig.WithRegion(o.Region))
	case os.Getenv("AWS_REGION") == "":
		opts = append(opts, awsconfig.WithRegion(cfg.Classifier.Bedrock.Region))
	}
	if o.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(o.Profile))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	var cl classify.Classifier
	switch o.Classifier {
	case "", "bedrock":
		cl, err = classify.NewBedrock(bedrockruntime.NewFromConfig(awsCfg), cfg)
	case "mock":
		cl, err = classify.NewMock(cfg)
	default:
		err = fmt.Errorf("unknown classifier %q", o.Classifier)
	}
	if err != nil {
		return nil, err
	}
	var sandboxTTL time.Duration
	if v := os.Getenv("ICR_SANDBOX_TTL"); v != "" {
		if sandboxTTL, err = time.ParseDuration(v); err != nil {
			return nil, fmt.Errorf("ICR_SANDBOX_TTL: %w", err)
		}
	}
	svc := &pipeline.Service{
		SandboxTTL: sandboxTTL,
		Cfg:        cfg,
		Classifier: cl,
		Store:      &store.Dynamo{Client: dynamodb.NewFromConfig(awsCfg), Table: o.Table},
		Router:     router.New(cfg),
		Actions:    &router.LogSink{W: o.ActionLog, Articles: cfg.Actions.KBArticles},
		Log:        o.Log,
	}
	if o.QueueURL != "" {
		svc.Queue = &ingest.SQSQueue{Client: sqs.NewFromConfig(awsCfg), QueueURL: o.QueueURL}
	}
	return &App{Svc: svc, AWS: awsCfg, Cfg: cfg}, nil
}
