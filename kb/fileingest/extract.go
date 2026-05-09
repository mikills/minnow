package fileingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	pdf "rsc.io/pdf"
)

type Page struct {
	Number int
	Text   string
}

func SourceText(ctx context.Context, path, contentType string) ([]Page, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch sourceContentType(path, contentType) {
	case "text/plain":
		return textPage(path)
	case "application/pdf":
		return PDFPages(ctx, path)
	default:
		return nil, fmt.Errorf("ingest: unsupported extracted content type %q", sourceContentType(path, contentType))
	}
}

func sourceContentType(path, contentType string) string {
	ct := normaliseContentType(contentType)
	if idx := strings.Index(ct, ";"); idx >= 0 {
		ct = strings.TrimSpace(ct[:idx])
	}
	if ct != "application/octet-stream" && ct != "" {
		return ct
	}
	if strings.ToLower(filepath.Ext(path)) == ".pdf" {
		return "application/pdf"
	}
	return "text/plain"
}

func textPage(path string) ([]Page, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := strings.TrimSpace(string(b))
	if text == "" {
		return nil, errors.New("ingest: extracted text is empty")
	}
	return []Page{{Number: 1, Text: text}}, nil
}

func PDFPages(ctx context.Context, path string) ([]Page, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Default().Warn("pdf file close failed", "error", closeErr)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	reader, err := pdf.NewReader(file, info.Size())
	if err != nil {
		return nil, fmt.Errorf("ingest: parse pdf: %w", err)
	}
	return openedPDFPages(ctx, reader)
}

func openedPDFPages(ctx context.Context, parsedPDF interface {
	NumPage() int
	Page(int) pdf.Page
}) ([]Page, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pages := make([]Page, 0, parsedPDF.NumPage())
	for i := 1; i <= parsedPDF.NumPage(); i++ {
		p := parsedPDF.Page(i)
		content := p.Content()
		var parts []string
		for _, txt := range content.Text {
			if s := strings.TrimSpace(txt.S); s != "" {
				parts = append(parts, s)
			}
		}
		pageText := strings.TrimSpace(strings.Join(parts, " "))
		if pageText == "" {
			continue
		}
		pages = append(pages, Page{Number: i, Text: pageText})
	}
	if len(pages) == 0 {
		return nil, errors.New("ingest: extracted text is empty")
	}
	return pages, nil
}

func normaliseContentType(contentType string) string {
	return strings.ToLower(strings.TrimSpace(contentType))
}
