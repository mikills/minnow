package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/mikills/minnow/kb"
)

type company struct {
	Ticker string
	CIK    string
}

var companies = []company{
	{"AAPL", "0000320193"},
	{"MSFT", "0000789019"},
	{"GOOGL", "0001652044"},
	{"AMZN", "0001018724"},
	{"NVDA", "0001045810"},
	{"META", "0001326801"},
	{"BRKA", "0001067983"},
	{"TSLA", "0001318605"},
	{"JPM", "0000019617"},
	{"WMT", "0000104169"},
	{"XOM", "0000034088"},
	{"JNJ", "0000200406"},
	{"V", "0001403161"},
	{"PG", "0000080424"},
	{"UNH", "0000731766"},
	{"MA", "0001141391"},
	{"HD", "0000354950"},
	{"BAC", "0000070858"},
	{"CVX", "0000093410"},
	{"KO", "0000021344"},
}

type submissionsJSON struct {
	Filings struct {
		Recent struct {
			Form            []string `json:"form"`
			AccessionNumber []string `json:"accessionNumber"`
			PrimaryDocument []string `json:"primaryDocument"`
			FilingDate      []string `json:"filingDate"`
		} `json:"recent"`
	} `json:"filings"`
}

func main() {
	outDir := flag.String("out", "testdata/corpus/10k-filings", "output directory")
	userAgent := flag.String("ua", "minnow-bench contact@example.com", "SEC EDGAR User-Agent (contact info required)")
	chunkSize := flag.Int("chunk", 1000, "approximate chunk size in characters")
	throttleMs := flag.Int("throttle-ms", 500, "sleep between companies to respect SEC rate limits")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		exit("mkdir: %v", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	chunker := kb.TextChunker{ChunkSize: *chunkSize}
	ctx := context.Background()

	totalChunks := 0
	failures := 0
	start := time.Now()

	for _, co := range companies {
		fmt.Printf("[%s] CIK %s\n", co.Ticker, co.CIK)
		chunks, filingDate, err := fetchOne(ctx, client, *userAgent, co, chunker)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  error: %v\n", err)
			failures++
			time.Sleep(time.Duration(*throttleMs) * time.Millisecond)
			continue
		}
		outPath := filepath.Join(*outDir, co.Ticker+".jsonl")
		if err := writeJSONL(outPath, chunks); err != nil {
			exit("write %s: %v", outPath, err)
		}
		fmt.Printf("  %d chunks, 10-K filed %s -> %s\n", len(chunks), filingDate, outPath)
		totalChunks += len(chunks)
		time.Sleep(time.Duration(*throttleMs) * time.Millisecond)
	}

	fmt.Printf("\n%d filings, %d chunks, %d failures, elapsed %v\n",
		len(companies)-failures, totalChunks, failures, time.Since(start).Round(time.Second))
}

func fetchOne(ctx context.Context, client *http.Client, ua string, co company, chunker kb.TextChunker) ([]kb.Chunk, string, error) {
	submissionsURL := fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", co.CIK)
	body, err := fetch(ctx, client, ua, submissionsURL)
	if err != nil {
		return nil, "", fmt.Errorf("submissions index: %w", err)
	}
	var subs submissionsJSON
	if err := json.Unmarshal(body, &subs); err != nil {
		return nil, "", fmt.Errorf("unmarshal submissions: %w", err)
	}
	r := subs.Filings.Recent
	for i := range r.Form {
		if r.Form[i] != "10-K" {
			continue
		}
		accession := strings.ReplaceAll(r.AccessionNumber[i], "-", "")
		cikTrimmed := strings.TrimLeft(co.CIK, "0")
		docURL := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
			cikTrimmed, accession, r.PrimaryDocument[i])
		raw, err := fetch(ctx, client, ua, docURL)
		if err != nil {
			return nil, "", fmt.Errorf("fetch 10-K: %w", err)
		}
		text, err := htmlToText(raw)
		if err != nil {
			return nil, "", fmt.Errorf("html to text: %w", err)
		}
		chunks, err := chunker.Chunk(ctx, co.Ticker, text)
		if err != nil {
			return nil, "", fmt.Errorf("chunk: %w", err)
		}
		return chunks, r.FilingDate[i], nil
	}
	return nil, "", fmt.Errorf("no 10-K in recent filings")
}

func fetch(ctx context.Context, client *http.Client, ua, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ua)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, url)
	}
	return io.ReadAll(resp.Body)
}

func htmlToText(data []byte) (string, error) {
	doc, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript":
				return
			}
		}
		if n.Type == html.TextNode {
			t := strings.TrimSpace(n.Data)
			if t != "" {
				sb.WriteString(t)
				sb.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
		if n.Type == html.ElementNode {
			switch n.Data {
			case "p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6":
				sb.WriteByte('\n')
			}
		}
	}
	walk(doc)
	return collapseWhitespace(sb.String()), nil
}

func collapseWhitespace(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	lastSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !lastSpace {
				out.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if r == '\n' {
			out.WriteByte('\n')
			lastSpace = true
			continue
		}
		if r == '\r' {
			continue
		}
		out.WriteRune(r)
		lastSpace = false
	}
	return out.String()
}

func writeJSONL(path string, chunks []kb.Chunk) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	for _, c := range chunks {
		rec := map[string]string{"id": c.ChunkID, "text": c.Text}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}

func exit(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
