package dashboard

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/marianina8/rivergate-icr-pipeline/internal/httplambda"
)

// Every HTML response must carry Content-Type when served through API
// Gateway — including error pages written with a non-200 status.
func TestHTMLContentTypeThroughLambdaAdapter(t *testing.T) {
	h := setup(t, "pw", false)
	lam := httplambda.Handler(h.h)
	post := func(path, body string) events.APIGatewayV2HTTPResponse {
		ev := events.APIGatewayV2HTTPRequest{RawPath: path, Body: body,
			Headers: map[string]string{"host": "x.execute-api.example", "content-type": "application/x-www-form-urlencoded"}}
		ev.RequestContext.HTTP.Method = "POST"
		resp, err := lam(context.Background(), ev)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := post("/login", url.Values{"password": {"wrong"}}.Encode())
	if resp.StatusCode != 401 || !strings.HasPrefix(resp.Headers["Content-Type"], "text/html") {
		t.Errorf("wrong-password page: %d %q", resp.StatusCode, resp.Headers["Content-Type"])
	}
	ok := post("/login", url.Values{"password": {"pw"}}.Encode())
	if len(ok.Cookies) != 1 {
		t.Fatal("no session cookie")
	}
	ev := events.APIGatewayV2HTTPRequest{RawPath: "/submit", Body: url.Values{"mode": {"json"}, "json": {"{nope"}}.Encode(),
		Headers: map[string]string{"host": "x.execute-api.example", "content-type": "application/x-www-form-urlencoded"},
		Cookies: []string{strings.SplitN(ok.Cookies[0], ";", 2)[0]}}
	ev.RequestContext.HTTP.Method = "POST"
	bad, _ := lam(context.Background(), ev)
	if bad.StatusCode != 400 || !strings.HasPrefix(bad.Headers["Content-Type"], "text/html") {
		t.Errorf("submit error page: %d %q", bad.StatusCode, bad.Headers["Content-Type"])
	}
}
