package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func buildCodeDocuments(ctx context.Context, root, repoID string, file codeScannedFile, opts CodeIndexOptions) ([]Document, []CodeChunkMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(file.AbsPath)
	if err != nil {
		return nil, nil, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" || isLikelyBinaryBytes(data) {
		return nil, nil, nil
	}
	chunks := chunkCodeText(text, file.Language, opts.ChunkSize, opts.ChunkOverlap)
	docs := make([]Document, 0, len(chunks))
	metas := make([]CodeChunkMetadata, 0, len(chunks))
	for _, chunk := range chunks {
		id := stableCodeChunkID(codeChunkIDInput{repoID: repoID, relPath: file.RelPath, startLine: chunk.StartLine, endLine: chunk.EndLine, fileHash: file.Hash, text: chunk.Text})
		meta := CodeChunkMetadata{ID: id, Path: file.RelPath, Hash: file.Hash, Language: file.Language, Symbol: chunk.Symbol, Kind: chunk.Kind, StartLine: chunk.StartLine, EndLine: chunk.EndLine}
		docs = append(docs, Document{ID: id, Text: formatCodeChunkText(file.RelPath, file.Language, chunk), Metadata: map[string]any{"code": meta}})
		metas = append(metas, meta)
	}
	return docs, metas, nil
}

func chunkCodeText(text, language string, chunkSize, overlap int) []codeChunk {
	lines := strings.Split(text, "\n")
	markers := symbolMarkers(lines, language)
	if len(markers) == 0 || len(lines) > 2000 {
		return lineChunks(lines, chunkSize, overlap, "", "")
	}
	var chunks []codeChunk
	for i, marker := range markers {
		start := marker.line
		end := len(lines)
		if i+1 < len(markers) {
			end = markers[i+1].line - 1
		}
		if start > end || start < 1 {
			continue
		}
		section := strings.Join(lines[start-1:end], "\n")
		if len(section) <= chunkSize {
			chunks = append(chunks, codeChunk{Text: strings.TrimSpace(section), Symbol: marker.symbol, Kind: marker.kind, StartLine: start, EndLine: end})
			continue
		}
		for _, c := range lineChunks(lines[start-1:end], chunkSize, overlap, marker.symbol, marker.kind) {
			c.StartLine += start - 1
			c.EndLine += start - 1
			chunks = append(chunks, c)
		}
	}
	return nonEmptyCodeChunks(chunks)
}

type codeSymbolMarker struct {
	line   int
	symbol string
	kind   string
}

var symbolRegexByLanguage = map[string][]struct {
	kind string
	re   *regexp.Regexp
}{
	"go": {
		{"function", regexp.MustCompile(`^func\s+(?:\([^)]*\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(`)},
		{"type", regexp.MustCompile(`^type\s+([A-Za-z_][A-Za-z0-9_]*)\s+`)},
	},
	"javascript": commonJSSymbolRegexes(),
	"typescript": commonJSSymbolRegexes(),
	"python": {
		{"function", regexp.MustCompile(`^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)},
		{"class", regexp.MustCompile(`^class\s+([A-Za-z_][A-Za-z0-9_]*)`)},
	},
	"rust": {
		{"function", regexp.MustCompile(`^(?:pub\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)},
		{"type", regexp.MustCompile(`^(?:pub\s+)?(?:struct|enum|trait)\s+([A-Za-z_][A-Za-z0-9_]*)`)},
	},
}

func symbolMarkers(lines []string, language string) []codeSymbolMarker {
	regexes := symbolRegexByLanguage[language]
	if len(regexes) == 0 {
		return nil
	}
	markers := make([]codeSymbolMarker, 0)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		for _, candidate := range regexes {
			match := candidate.re.FindStringSubmatch(trimmed)
			if len(match) >= 2 {
				markers = append(markers, codeSymbolMarker{line: i + 1, symbol: match[1], kind: candidate.kind})
				break
			}
		}
	}
	return markers
}

func lineChunks(lines []string, chunkSize, overlap int, symbol, kind string) []codeChunk {
	var chunks []codeChunk
	start := 0
	for start < len(lines) {
		if len(lines[start])+1 > chunkSize {
			chunks = append(chunks, splitLongLineChunks(longLineChunkInput{line: lines[start], lineNo: start + 1, chunkSize: chunkSize, overlap: overlap, symbol: symbol, kind: kind})...)
			start++
			continue
		}
		end := codeChunkEnd(lines, start, chunkSize)
		if chunk := codeChunkFromLines(lines, start, end, symbol, kind); chunk.Text != "" {
			chunks = append(chunks, chunk)
		}
		if end >= len(lines) {
			break
		}
		start = nextCodeChunkStart(lines[start:end], end, overlap)
	}
	return chunks
}

func codeChunkEnd(lines []string, start int, chunkSize int) int {
	end := start
	length := 0
	for end < len(lines) {
		lineLen := len(lines[end]) + 1
		if end > start && length+lineLen > chunkSize {
			break
		}
		length += lineLen
		end++
	}
	if end == start {
		return end + 1
	}
	return end
}

func codeChunkFromLines(lines []string, start int, end int, symbol string, kind string) codeChunk {
	text := strings.TrimSpace(strings.Join(lines[start:end], "\n"))
	if text == "" {
		return codeChunk{}
	}
	return codeChunk{Text: text, Symbol: symbol, Kind: kind, StartLine: start + 1, EndLine: end}
}

func nextCodeChunkStart(window []string, end int, overlap int) int {
	start := end - overlapLines(window, overlap)
	if start < 0 || start >= end {
		return end
	}
	return start
}

type longLineChunkInput struct {
	line      string
	lineNo    int
	chunkSize int
	overlap   int
	symbol    string
	kind      string
}

func splitLongLineChunks(input longLineChunkInput) []codeChunk {
	line := strings.TrimSpace(input.line)
	if line == "" {
		return nil
	}
	step := input.chunkSize - input.overlap
	if step <= 0 {
		step = input.chunkSize
	}
	var chunks []codeChunk
	for start := 0; start < len(line); {
		end := start + input.chunkSize
		if end > len(line) {
			end = len(line)
		}
		text := strings.TrimSpace(line[start:end])
		if text != "" {
			chunks = append(chunks, codeChunk{Text: text, Symbol: input.symbol, Kind: input.kind, StartLine: input.lineNo, EndLine: input.lineNo})
		}
		if end >= len(line) {
			break
		}
		start += step
	}
	return chunks
}

func overlapLines(lines []string, overlapChars int) int {
	if overlapChars <= 0 || len(lines) == 0 {
		return 0
	}
	total := 0
	count := 0
	for i := len(lines) - 1; i >= 0; i-- {
		total += len(lines[i]) + 1
		count++
		if total >= overlapChars {
			break
		}
	}
	return count
}

func nonEmptyCodeChunks(in []codeChunk) []codeChunk {
	out := in[:0]
	for _, c := range in {
		if strings.TrimSpace(c.Text) != "" {
			out = append(out, c)
		}
	}
	return out
}

func formatCodeChunkText(path, language string, chunk codeChunk) string {
	var b strings.Builder
	b.WriteString("path: ")
	b.WriteString(path)
	b.WriteString("\n")
	if language != "" {
		b.WriteString("language: ")
		b.WriteString(language)
		b.WriteString("\n")
	}
	if chunk.Symbol != "" {
		b.WriteString("symbol: ")
		b.WriteString(chunk.Symbol)
		b.WriteString("\n")
	}
	b.WriteString(fmt.Sprintf("lines: %d-%d\n\n", chunk.StartLine, chunk.EndLine))
	b.WriteString(chunk.Text)
	return b.String()
}

type codeChunkIDInput struct {
	repoID    string
	relPath   string
	startLine int
	endLine   int
	fileHash  string
	text      string
}

func stableCodeChunkID(input codeChunkIDInput) string {
	sum := sha256.Sum256([]byte(input.repoID + "\x00" + input.relPath + "\x00" + fmt.Sprint(input.startLine) + "\x00" + fmt.Sprint(input.endLine) + "\x00" + input.fileHash + "\x00" + input.text))
	pathToken := sanitizeCodeIDToken(input.relPath)
	if len(pathToken) > 80 {
		pathToken = pathToken[len(pathToken)-80:]
	}
	return fmt.Sprintf("code-%s-%d-%d-%s", pathToken, input.startLine, input.endLine, hex.EncodeToString(sum[:8]))
}

func sanitizeCodeIDToken(s string) string {
	s = strings.Trim(filepath.ToSlash(s), "/")
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.':
			return r
		default:
			return '-'
		}
	}, s)
}

var codeLanguageByExt = map[string]string{
	".go":    "go",
	".js":    "javascript",
	".jsx":   "javascript",
	".mjs":   "javascript",
	".cjs":   "javascript",
	".ts":    "typescript",
	".tsx":   "typescript",
	".py":    "python",
	".rs":    "rust",
	".java":  "java",
	".rb":    "ruby",
	".php":   "php",
	".c":     "c",
	".h":     "c",
	".cc":    "cpp",
	".cpp":   "cpp",
	".cxx":   "cpp",
	".hpp":   "cpp",
	".cs":    "csharp",
	".swift": "swift",
	".kt":    "kotlin",
	".kts":   "kotlin",
	".md":    "markdown",
	".mdx":   "markdown",
	".yaml":  "yaml",
	".yml":   "yaml",
	".json":  "json",
	".sh":    "shell",
	".bash":  "shell",
	".zsh":   "shell",
}

func detectCodeLanguage(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if language, ok := codeLanguageByExt[ext]; ok {
		return language
	}
	base := strings.ToLower(filepath.Base(path))
	if base == "dockerfile" || strings.HasPrefix(base, "dockerfile.") {
		return "dockerfile"
	}
	return strings.TrimPrefix(ext, ".")
}

func commonJSSymbolRegexes() []struct {
	kind string
	re   *regexp.Regexp
} {
	return []struct {
		kind string
		re   *regexp.Regexp
	}{
		{"function", regexp.MustCompile(`^(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(`)},
		{"class", regexp.MustCompile(`^(?:export\s+)?class\s+([A-Za-z_$][A-Za-z0-9_$]*)`)},
		{"function", regexp.MustCompile(`^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?:async\s*)?\(?`)},
	}
}
