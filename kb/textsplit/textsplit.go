package textsplit

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

var defaultSeparators = []string{"\n\n", "\n", ".", " ", ""}

const DefaultChunkSize = 500

type Chunk struct {
	DocID   string
	ChunkID string
	Text    string
	Start   int
	End     int
}

type Splitter struct{ ChunkSize int }

func (s Splitter) Chunk(ctx context.Context, docID string, text string) ([]Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, nil
	}
	chunkSize := s.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	pieces := recursiveSplit(trimmed, defaultSeparators, chunkSize)
	return buildChunks(ctx, docID, text, pieces)
}

func buildChunks(ctx context.Context, docID string, sourceText string, pieces []string) ([]Chunk, error) {
	chunks := make([]Chunk, 0, len(pieces))
	pos := 0
	for i, piece := range pieces {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		span := findSpan(sourceText, piece, pos)
		chunks = append(
			chunks,
			Chunk{
				DocID:   docID,
				ChunkID: fmt.Sprintf("%s-chunk-%03d", docID, i),
				Text:    piece,
				Start:   span.start,
				End:     span.end,
			},
		)
		pos = span.end
	}
	return chunks, nil
}

type span struct{ start, end int }

func findSpan(sourceText string, piece string, pos int) span {
	sourceLen := len(sourceText)
	pos = clamp(pos, 0, sourceLen)
	idx := strings.Index(sourceText[pos:], piece)
	if idx < 0 {
		idx = fallbackIndex(sourceText, piece, pos)
	}
	start := clamp(pos+idx, 0, sourceLen)
	end := clamp(start+len(piece), start, sourceLen)
	return span{start: start, end: end}
}

func fallbackIndex(sourceText string, piece string, pos int) int {
	absoluteIdx := strings.Index(sourceText, piece)
	if absoluteIdx < 0 {
		return 0
	}
	return absoluteIdx - pos
}

func clamp(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func recursiveSplit(text string, separators []string, chunkSize int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if len(text) <= chunkSize {
		return []string{text}
	}
	if len(separators) == 0 {
		return hardSplitByRunes(text, chunkSize)
	}
	sep := separators[0]
	remainingSeps := separators[1:]
	if sep == "" {
		return hardSplitByRunes(text, chunkSize)
	}
	parts := strings.Split(text, sep)
	merged := mergeSmallPieces(parts, sep, chunkSize, sep == ".")
	var result []string
	for _, piece := range merged {
		piece = strings.TrimSpace(piece)
		if piece == "" {
			continue
		}
		if len(piece) <= chunkSize {
			result = append(result, piece)
		} else {
			result = append(result, recursiveSplit(piece, remainingSeps, chunkSize)...)
		}
	}
	return result
}

func mergeSmallPieces(parts []string, sep string, chunkSize int, keepSepLeft bool) []string {
	if len(parts) == 0 {
		return nil
	}
	var result []string
	var current strings.Builder
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if current.Len() == 0 {
			current.WriteString(part)
			continue
		}
		if canMerge(current.Len(), len(part), len(sep), chunkSize, keepSepLeft) {
			writeMergedPiece(&current, part, sep, keepSepLeft)
			continue
		}
		if keepSepLeft {
			current.WriteString(sep)
		}
		result = append(result, current.String())
		current.Reset()
		current.WriteString(part)
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

func canMerge(currentLen, partLen, sepLen, chunkSize int, keepSepLeft bool) bool {
	if keepSepLeft {
		return currentLen+sepLen+1+partLen <= chunkSize
	}
	return currentLen+sepLen+partLen <= chunkSize
}

func writeMergedPiece(current *strings.Builder, part string, sep string, keepSepLeft bool) {
	current.WriteString(sep)
	if keepSepLeft {
		current.WriteString(" ")
	}
	current.WriteString(part)
}

func hardSplitByRunes(text string, chunkSize int) []string {
	if len(text) <= chunkSize {
		return []string{text}
	}
	var result []string
	var current strings.Builder
	for _, r := range text {
		runeBytes := utf8.RuneLen(r)
		if current.Len() > 0 && current.Len()+runeBytes > chunkSize {
			result = append(result, current.String())
			current.Reset()
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		if s := strings.TrimSpace(current.String()); s != "" {
			result = append(result, s)
		}
	}
	return result
}
