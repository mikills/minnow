package mcpserver

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func NewHTTPHandler(server *mcp.Server, cfg Config) http.Handler {
	cfg = cfg.normalized()
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		Stateless:    cfg.HTTPStateless,
		JSONResponse: cfg.HTTPJSONResponse,
	})
}

func RunStdio(ctx context.Context, server *mcp.Server) error {
	return server.Run(ctx, &mcp.StdioTransport{})
}
