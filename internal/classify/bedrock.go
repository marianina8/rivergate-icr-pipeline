package classify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
)

// ConverseAPI is the one Bedrock Runtime call this package uses. Declared as
// an interface so tests can substitute a fake — no AWS calls in tests.
type ConverseAPI interface {
	Converse(ctx context.Context, in *bedrockruntime.ConverseInput, opts ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error)
}

// Bedrock classifies tickets with a single Bedrock Converse call.
//
// PHASE 5: this compiles and is unit-tested against a fake client, but has
// never been run against live Bedrock. Select it with classifier.provider:
// bedrock (or --classifier bedrock) once AWS access is available.
type Bedrock struct {
	client ConverseAPI
	cfg    config.Bedrock
	prompt *Prompt
	tx     Taxonomy
	now    func() time.Time
}

// NewBedrock wires a classifier around an existing Converse client.
func NewBedrock(client ConverseAPI, c *config.Config) (*Bedrock, error) {
	p, err := NewPrompt(c)
	if err != nil {
		return nil, err
	}
	if c.Classifier.Bedrock.ModelID == "" {
		return nil, fmt.Errorf("classifier.bedrock.model_id is required")
	}
	return &Bedrock{client: client, cfg: c.Classifier.Bedrock, prompt: p, tx: TaxonomyFrom(c), now: time.Now}, nil
}

// NewBedrockFromEnv loads AWS credentials from the default chain (env vars,
// ~/.aws, Lambda role). Loading config makes no network call; the first
// Classify does.
func NewBedrockFromEnv(ctx context.Context, c *config.Config) (*Bedrock, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Classifier.Bedrock.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return NewBedrock(bedrockruntime.NewFromConfig(awsCfg), c)
}

// Name identifies this classifier (and model) in the audit trail.
func (b *Bedrock) Name() string { return "bedrock:" + b.cfg.ModelID }

// Classify sends one structured prompt and strictly parses the JSON reply.
func (b *Bedrock) Classify(ctx context.Context, t ingest.Ticket) (Classification, error) {
	sys, err := b.prompt.System()
	if err != nil {
		return Classification{}, err
	}
	usr, err := b.prompt.User(t)
	if err != nil {
		return Classification{}, err
	}
	maxTokens := b.cfg.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 400
	}
	out, err := b.client.Converse(ctx, &bedrockruntime.ConverseInput{
		ModelId: aws.String(b.cfg.ModelID),
		System:  []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: sys}},
		Messages: []types.Message{{
			Role:    types.ConversationRoleUser,
			Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: usr}},
		}},
		InferenceConfig: &types.InferenceConfiguration{
			MaxTokens:   aws.Int32(maxTokens),
			Temperature: aws.Float32(b.cfg.Temperature),
		},
	})
	if err != nil {
		return Classification{}, fmt.Errorf("bedrock converse: %w", err)
	}
	msg, ok := out.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return Classification{}, fmt.Errorf("%w: unexpected Converse output type %T", ErrBadOutput, out.Output)
	}
	var text strings.Builder
	for _, block := range msg.Value.Content {
		if tb, ok := block.(*types.ContentBlockMemberText); ok {
			text.WriteString(tb.Value)
		}
	}
	c, err := ParseModelOutput(text.String(), b.tx)
	if err != nil {
		return Classification{}, err
	}
	c.Classifier = b.Name()
	c.ClassifiedAt = b.now().UTC()
	return c, nil
}
