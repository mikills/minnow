package codeindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type ScannedFile struct {
	AbsPath   string
	RelPath   string
	Hash      string
	SizeBytes int64
	Language  string
}

type ChunkMetadata struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Language  string `json:"language,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
	Kind      string `json:"kind,omitempty"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

type Document struct {
	ID       string
	Text     string
	Metadata map[string]any
}

type Chunk struct {
	Text      string
	Symbol    string
	Kind      string
	StartLine int
	EndLine   int
}

func BuildDocuments(
	ctx context.Context,
	root, repoID string,
	file ScannedFile,
	opts Options,
) ([]Document, []ChunkMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(file.AbsPath)
	if err != nil {
		return nil, nil, err
	}
	text := strings.TrimSpace(string(data))
	if text == "" || IsLikelyBinaryBytes(data) {
		return nil, nil, nil
	}
	chunks := ChunkText(text, file.Language, opts.ChunkSize, opts.ChunkOverlap)
	docs := make([]Document, 0, len(chunks))
	metas := make([]ChunkMetadata, 0, len(chunks))
	for _, chunk := range chunks {
		id := StableChunkID(
			ChunkIDInput{
				RepoID:    repoID,
				RelPath:   file.RelPath,
				StartLine: chunk.StartLine,
				EndLine:   chunk.EndLine,
				FileHash:  file.Hash,
				Text:      chunk.Text,
			},
		)
		meta := ChunkMetadata{
			ID:        id,
			Path:      file.RelPath,
			Hash:      file.Hash,
			Language:  file.Language,
			Symbol:    chunk.Symbol,
			Kind:      chunk.Kind,
			StartLine: chunk.StartLine,
			EndLine:   chunk.EndLine,
		}
		docs = append(
			docs,
			Document{
				ID:       id,
				Text:     FormatChunkText(file.RelPath, file.Language, chunk),
				Metadata: map[string]any{"code": meta},
			},
		)
		metas = append(metas, meta)
	}
	return docs, metas, nil
}

func ChunkText(text, language string, chunkSize, overlap int) []Chunk {
	lines := strings.Split(text, "\n")
	markers := symbolMarkers(lines, language)
	if len(markers) == 0 || len(lines) > 2000 {
		return lineChunks(lines, chunkSize, overlap, "", "")
	}
	var chunks []Chunk
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
			chunks = append(
				chunks,
				Chunk{
					Text:      strings.TrimSpace(section),
					Symbol:    marker.symbol,
					Kind:      marker.kind,
					StartLine: start,
					EndLine:   end,
				},
			)
			continue
		}
		for _, c := range lineChunks(lines[start-1:end], chunkSize, overlap, marker.symbol, marker.kind) {
			c.StartLine += start - 1
			c.EndLine += start - 1
			chunks = append(chunks, c)
		}
	}
	return nonEmptyChunks(chunks)
}

type symbolMarker struct {
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

func symbolMarkers(lines []string, language string) []symbolMarker {
	regexes := symbolRegexByLanguage[language]
	if len(regexes) == 0 {
		return nil
	}
	markers := make([]symbolMarker, 0, estimatedSymbolMarkers(len(lines)))
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		for _, candidate := range regexes {
			match := candidate.re.FindStringSubmatch(trimmed)
			if len(match) >= 2 {
				markers = append(markers, symbolMarker{line: i + 1, symbol: match[1], kind: candidate.kind})
				break
			}
		}
	}
	return markers
}

func estimatedSymbolMarkers(lineCount int) int {
	estimate := lineCount / 16
	if estimate < 8 {
		return 8
	}
	return estimate
}

func lineChunks(lines []string, chunkSize, overlap int, symbol, kind string) []Chunk {
	var chunks []Chunk
	start := 0
	for start < len(lines) {
		if len(lines[start])+1 > chunkSize {
			chunks = append(
				chunks,
				splitLongLineChunks(
					longLineChunkInput{
						line:      lines[start],
						lineNo:    start + 1,
						chunkSize: chunkSize,
						overlap:   overlap,
						symbol:    symbol,
						kind:      kind,
					},
				)...)
			start++
			continue
		}
		end := chunkEnd(lines, start, chunkSize)
		if chunk := chunkFromLines(lines, start, end, symbol, kind); chunk.Text != "" {
			chunks = append(chunks, chunk)
		}
		if end >= len(lines) {
			break
		}
		start = nextChunkStart(lines[start:end], end, overlap)
	}
	return chunks
}

func chunkEnd(lines []string, start int, chunkSize int) int {
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

func chunkFromLines(lines []string, start int, end int, symbol string, kind string) Chunk {
	text := joinTrimmedLines(lines[start:end])
	if text == "" {
		return Chunk{}
	}
	return Chunk{Text: text, Symbol: symbol, Kind: kind, StartLine: start + 1, EndLine: end}
}

func joinTrimmedLines(lines []string) string {
	first := 0
	for first < len(lines) && strings.TrimSpace(lines[first]) == "" {
		first++
	}
	last := len(lines) - 1
	for last >= first && strings.TrimSpace(lines[last]) == "" {
		last--
	}
	if first > last {
		return ""
	}
	return strings.Join(lines[first:last+1], "\n")
}

func nextChunkStart(window []string, end int, overlap int) int {
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

func splitLongLineChunks(input longLineChunkInput) []Chunk {
	line := strings.TrimSpace(input.line)
	if line == "" {
		return nil
	}
	step := input.chunkSize - input.overlap
	if step <= 0 {
		step = input.chunkSize
	}
	var chunks []Chunk
	for start := 0; start < len(line); {
		end := min(start+input.chunkSize, len(line))
		text := strings.TrimSpace(line[start:end])
		if text != "" {
			chunks = append(
				chunks,
				Chunk{
					Text:      text,
					Symbol:    input.symbol,
					Kind:      input.kind,
					StartLine: input.lineNo,
					EndLine:   input.lineNo,
				},
			)
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

func nonEmptyChunks(in []Chunk) []Chunk {
	out := in[:0]
	for _, c := range in {
		if strings.TrimSpace(c.Text) != "" {
			out = append(out, c)
		}
	}
	return out
}

func FormatChunkText(path, language string, chunk Chunk) string {
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
	b.WriteString("lines: ")
	b.WriteString(strconv.Itoa(chunk.StartLine))
	b.WriteString("-")
	b.WriteString(strconv.Itoa(chunk.EndLine))
	b.WriteString("\n\n")
	b.WriteString(chunk.Text)
	return b.String()
}

type ChunkIDInput struct {
	RepoID    string
	RelPath   string
	StartLine int
	EndLine   int
	FileHash  string
	Text      string
}

func StableChunkID(input ChunkIDInput) string {
	var hashInput strings.Builder
	hashInput.Grow(len(input.RepoID) + len(input.RelPath) + len(input.FileHash) + len(input.Text) + 32)
	hashInput.WriteString(input.RepoID)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.RelPath)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(strconv.Itoa(input.StartLine))
	hashInput.WriteByte('\x00')
	hashInput.WriteString(strconv.Itoa(input.EndLine))
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.FileHash)
	hashInput.WriteByte('\x00')
	hashInput.WriteString(input.Text)
	sum := sha256.Sum256([]byte(hashInput.String()))
	pathToken := SanitizeIDToken(input.RelPath)
	if len(pathToken) > 80 {
		pathToken = pathToken[len(pathToken)-80:]
	}
	return fmt.Sprintf("code-%s-%d-%d-%s", pathToken, input.StartLine, input.EndLine, hex.EncodeToString(sum[:8]))
}

func SanitizeIDToken(s string) string {
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

var LanguageByExt = map[string]string{
	".go": "go", ".js": "javascript", ".jsx": "javascript", ".mjs": "javascript", ".cjs": "javascript", ".ts": "typescript", ".tsx": "typescript", ".py": "python", ".rs": "rust", ".java": "java", ".rb": "ruby", ".php": "php", ".c": "c", ".h": "c", ".cc": "cpp", ".cpp": "cpp", ".cxx": "cpp", ".hpp": "cpp", ".cs": "csharp", ".swift": "swift", ".kt": "kotlin", ".kts": "kotlin", ".md": "markdown", ".mdx": "markdown", ".yaml": "yaml", ".yml": "yaml", ".json": "json", ".sh": "shell", ".bash": "shell", ".zsh": "shell",
}

func DetectLanguage(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if language, ok := LanguageByExt[ext]; ok {
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
		{
			"function",
			regexp.MustCompile(`^(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?:async\s*)?\(?`),
		},
	}
}
