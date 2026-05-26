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
	pieces := splitText(trimmed, chunkSize)
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

func splitText(text string, chunkSize int) []string {
	pieces := make([]string, 0, max(1, len(text)/chunkSize))
	recursiveSplitInto(&pieces, text, defaultSeparators, chunkSize)
	return pieces
}

func recursiveSplitInto(out *[]string, text string, separators []string, chunkSize int) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}
	if len(text) <= chunkSize {
		*out = append(*out, text)
		return
	}
	if len(separators) == 0 || separators[0] == "" {
		*out = append(*out, hardSplitByRunes(text, chunkSize)...)
		return
	}
	sep := separators[0]
	parts := strings.Split(text, sep)
	mergeSplitPartsInto(splitMergeInput{
		out:           out,
		parts:         parts,
		sep:           sep,
		remainingSeps: separators[1:],
		chunkSize:     chunkSize,
		keepSepLeft:   sep == ".",
	})
}

type splitMergeInput struct {
	out           *[]string
	parts         []string
	sep           string
	remainingSeps []string
	chunkSize     int
	keepSepLeft   bool
}

func mergeSplitPartsInto(input splitMergeInput) {
	var current strings.Builder
	current.Grow(input.chunkSize)
	for _, part := range input.parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if current.Len() == 0 {
			current.WriteString(part)
			continue
		}
		if canMerge(current.Len(), len(part), len(input.sep), input.chunkSize, input.keepSepLeft) {
			writeMergedPiece(&current, part, input.sep, input.keepSepLeft)
			continue
		}
		if input.keepSepLeft {
			current.WriteString(input.sep)
		}
		flushSplitPiece(input.out, current.String(), input.remainingSeps, input.chunkSize)
		current.Reset()
		current.WriteString(part)
	}
	if current.Len() > 0 {
		flushSplitPiece(input.out, current.String(), input.remainingSeps, input.chunkSize)
	}
}

func flushSplitPiece(out *[]string, piece string, remainingSeps []string, chunkSize int) {
	piece = strings.TrimSpace(piece)
	if piece == "" {
		return
	}
	if len(piece) <= chunkSize {
		*out = append(*out, piece)
		return
	}
	recursiveSplitInto(out, piece, remainingSeps, chunkSize)
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
