package api

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"golang.org/x/oauth2"
	"resty.dev/v3"
)

const defaultAPIVersion = "v65.0"

// normalizeAPIVersion accepts versions in the form "XX.Y", "vXX.Y", "vXX", or "XX"
// and normalizes to "vXX.Y" (appending ".0" if no minor version is present).
func normalizeAPIVersion(v string) string {
	v = strings.TrimPrefix(v, "v")
	if !strings.Contains(v, ".") {
		v += ".0"
	}
	return "v" + v
}

type Client struct {
	config      *types.AuthConfig
	http        *resty.Client
	logger      *slog.Logger
	tokenSource oauth2.TokenSource
}

type Option func(*Client)

func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

// WithModifyClient allows tweaking settings on the underlying *resty.Client.
// Mainly used for testing
func WithModifyClient(mod func(*resty.Client)) Option {
	return func(c *Client) {
		mod(c.http)
	}
}

func NewClient(cfg *types.AuthConfig, opts ...Option) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("auth config is required")
	}
	if cfg.APIVersion == "" {
		cfg.APIVersion = defaultAPIVersion
	} else {
		cfg.APIVersion = normalizeAPIVersion(cfg.APIVersion)
	}

	c := new(Client)
	c.config = cfg
	c.logger = slog.Default()
	// default resty client. can be modified with WithModifyClient option, mainly for testing (e.g. to set a custom base URL or auth token)
	c.http = resty.New().
		SetDebug(false).           // TODO: toggle as needed
		SetDebugLogFormatter(nil). // force use slog
		OnDebugLog(func(dl *resty.DebugLog) {
			c.logger.Debug(
				"http",
				slog.Any("request", dl.Request),
				slog.Any("response", dl.Response),
				slog.Any("trace-info", dl.TraceInfo),
			)
		}).
		SetHeader("X-Salesforce-Partner-Name", "dbt_labs"). // TODO: make configurable
		// SetHeader("User-Agent", "sf-d360-api/dev (go)").     // TODO: this could be more canonical
		AddResponseMiddleware(func(_ *resty.Client, resp *resty.Response) error {
			rlInfo := resp.Header().Values("Sforce-Limit-Info")
			if len(rlInfo) > 0 {
				c.logger.Info(
					"API usage info",
					slog.String("endpoint", resp.Request.URL),
					slog.String("method", resp.Request.Method),
					slog.Any("limit-info", rlInfo),
				)
			}
			return nil
		})

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// SetBaseURL overrides the instance URL used for API requests.
// Intended for use in tests (replay mode).
func (c *Client) SetBaseURL(url string) { c.http.SetBaseURL(url) }

// SetAuthToken overrides the Bearer token used for API requests.
// Intended for use in tests (replay mode).
func (c *Client) SetAuthToken(token string) { c.http.SetAuthToken(token) }

func (c *Client) Close() {
	if c.http != nil {
		c.http.Close()
	}
}
func (c *Client) GetLogger() *slog.Logger {
	return c.logger
}

// request returns a new resty request with the context and API version path param pre-set.
// Callers use full path templates: c.request(ctx).Get("/services/data/{version}/ssot/metadata")
func (c *Client) request(ctx context.Context) *resty.Request {
	return c.http.R().
		SetContext(ctx).
		SetPathParam("version", c.config.APIVersion)
}
