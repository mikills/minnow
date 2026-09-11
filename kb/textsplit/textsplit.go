package textsplit

import (
	"context"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Separators are tried in priority order when choosing a chunk boundary.
// The last occurrence within the size window wins, so larger semantic units
// (paragraphs) are preferred over lines, sentences, and words.
var defaultSeparators = []string{"\n\n", "\n", ".", " "}

const (
	// DefaultChunkSize targets the middle of the 512-1024 token sweet
	// spot reported for RAG retrieval, converted to characters at
	// roughly 4 chars per token for English text.
	DefaultChunkSize = 3000
	// DefaultChunkOverlap is 10% of DefaultChunkSize, matching the
	// code-index chunker ratio (120/1200) and the commonly recommended
	// 10-20% overlap window.
	DefaultChunkOverlap = 300
)

type Chunk struct {
	DocID   string
	ChunkID string
	Text    string
	Start   int
	End     int
}

type Splitter struct {
	ChunkSize int
	// ChunkOverlap is the number of bytes repeated from the previous chunk
	// at the start of the next one. Zero selects DefaultChunkOverlap;
	// negative disables overlap.
	ChunkOverlap int
}

func (s Splitter) Chunk(ctx context.Context, docID string, text string) ([]Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	chunkSize, overlap := normalize(s.ChunkSize, s.ChunkOverlap)
	return splitWithOverlap(ctx, docID, text, chunkSize, overlap)
}

func normalize(chunkSize, overlap int) (int, int) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	switch {
	case overlap < 0:
		overlap = 0
	case overlap == 0:
		overlap = DefaultChunkOverlap
	}
	if overlap >= chunkSize {
		overlap = chunkSize / 10
	}
	return chunkSize, overlap
}

// splitWithOverlap walks the source with a sliding window. Each chunk holds
// up to chunkSize bytes cut at the best separator boundary, and the next
// chunk starts overlap bytes before the previous chunk's trimmed end
// (snapped back to a word boundary). Chunk text is always an exact slice of
// the source, so Text == source[Start:End] by construction.
func splitWithOverlap(ctx context.Context, docID, source string, chunkSize, overlap int) ([]Chunk, error) {
	n := len(source)
	pos := skipWhitespaceForward(source, 0)
	var chunks []Chunk
	prevEnd := -1
	for pos < n {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(strings.TrimSpace(source[pos:])) <= chunkSize {
			end := trimTrailingWhitespace(source, pos, n)
			if end <= pos {
				break
			}
			chunks = append(chunks, Chunk{
				DocID:   docID,
				ChunkID: fmt.Sprintf("%s-chunk-%03d", docID, len(chunks)),
				Text:    source[pos:end],
				Start:   pos,
				End:     end,
			})
			break
		}
		windowEnd := pos + chunkSize
		if windowEnd > n {
			windowEnd = n
		} else {
			windowEnd = backToRuneBoundary(source, pos, windowEnd)
		}
		split := bestSplit(source, pos, windowEnd)
		if split <= pos {
			// Never emit an empty chunk; a single rune always fits even
			// when it exceeds a tiny chunkSize.
			split = advanceOneRune(source, pos)
		}
		end := trimTrailingWhitespace(source, pos, split)
		if end <= pos {
			pos = skipWhitespaceForward(source, split)
			continue
		}
		if prevEnd >= 0 && end <= prevEnd {
			// Fully contained in the previous chunk (the overlapped
			// window re-selected an earlier boundary), so skip past it
			// without emitting a duplicate. Coverage is preserved
			// because [pos:end] lies inside the previous chunk.
			pos = skipWhitespaceForward(source, split)
			continue
		}
		chunks = append(chunks, Chunk{
			DocID:   docID,
			ChunkID: fmt.Sprintf("%s-chunk-%03d", docID, len(chunks)),
			Text:    source[pos:end],
			Start:   pos,
			End:     end,
		})
		prevEnd = end
		next := end
		if overlap > 0 && end-pos > overlap {
			candidate := forwardToRuneBoundary(source, end-overlap, end)
			candidate = backToWordStart(source, pos, candidate)
			if candidate > pos && candidate < end {
				next = candidate
			}
		}
		if next <= pos {
			next = end
		}
		pos = skipWhitespaceForward(source, next)
	}
	return chunks, nil
}

// bestSplit returns the end offset of the current chunk: the last occurrence
// of the highest-priority separator within [pos, windowEnd), or windowEnd as
// a hard cut. The result never exceeds windowEnd, keeping chunks within
// chunkSize, and always lands on a rune boundary (separators are ASCII).
func bestSplit(source string, pos, windowEnd int) int {
	for _, sep := range defaultSeparators {
		rel := strings.LastIndex(source[pos:windowEnd], sep)
		if rel < 0 {
			continue
		}
		if split := pos + rel + len(sep); split > pos && split <= windowEnd {
			return split
		}
	}
	return windowEnd
}

func skipWhitespaceForward(source string, i int) int {
	for i < len(source) {
		r, size := utf8.DecodeRuneInString(source[i:])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		if !unicode.IsSpace(r) {
			break
		}
		i += size
	}
	return i
}

func trimTrailingWhitespace(source string, pos, end int) int {
	e := end
	for e > pos {
		r, size := utf8.DecodeLastRuneInString(source[pos:e])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		if !unicode.IsSpace(r) {
			break
		}
		e -= size
	}
	return e
}

func backToRuneBoundary(source string, pos, i int) int {
	if i >= len(source) {
		return len(source)
	}
	for i > pos && !utf8.RuneStart(source[i]) {
		i--
	}
	return i
}

func forwardToRuneBoundary(source string, i, limit int) int {
	for i < limit && i < len(source) && !utf8.RuneStart(source[i]) {
		i++
	}
	return i
}

// backToWordStart moves idx back to just after the previous ASCII whitespace
// so overlap starts on a word boundary. Without whitespace it returns idx
// (hard cut inside a long token).
func backToWordStart(source string, lo, idx int) int {
	if idx <= lo {
		return idx
	}
	if idx > len(source) {
		idx = len(source)
	}
	if j := strings.LastIndexAny(source[lo:idx], " \t\n\r\f\v"); j >= 0 {
		return lo + j + 1
	}
	return idx
}

func advanceOneRune(source string, pos int) int {
	if pos >= len(source) {
		return len(source)
	}
	_, size := utf8.DecodeRuneInString(source[pos:])
	if size <= 0 {
		return pos + 1
	}
	return pos + size
}
