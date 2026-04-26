package mcpserver

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerResources(server *mcp.Server, s *Service) {
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "minnow://operations/{event_id}",
		Name:        "operation_status",
		Description: "Read a Minnow operation status by event ID.",
		MIMEType:    "application/json",
	}, s.readResource)
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "minnow://media/{media_id}",
		Name:        "media_metadata",
		Description: "Read Minnow media metadata by media ID.",
		MIMEType:    "application/json",
	}, s.readResource)
}

func (s *Service) readResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	uri := req.Params.URI
	var v any
	var err error
	switch {
	case strings.HasPrefix(uri, "minnow://operations/"):
		id := strings.TrimPrefix(uri, "minnow://operations/")
		v, err = s.operationStatusOutput(ctx, id)
	case strings.HasPrefix(uri, "minnow://media/"):
		if s.GetMedia == nil {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		id := strings.TrimPrefix(uri, "minnow://media/")
		v, err = s.GetMedia(ctx, id)
	default:
		return nil, mcp.ResourceNotFoundError(uri)
	}
	if err != nil {
		if isNotFound(err) {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		return nil, err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(data)}}}, nil
}
