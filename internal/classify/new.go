package classify

import (
	"context"
	"fmt"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
)

// New returns the classifier named by provider ("mock" or "bedrock"). An empty
// provider uses the config default.
func New(ctx context.Context, provider string, c *config.Config) (Classifier, error) {
	if provider == "" {
		provider = c.Classifier.Provider
	}
	switch provider {
	case "", "mock":
		return NewMock(c)
	case "bedrock":
		return NewBedrockFromEnv(ctx, c)
	default:
		return nil, fmt.Errorf("unknown classifier %q (want mock or bedrock)", provider)
	}
}
