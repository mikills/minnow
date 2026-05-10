package localembed

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

const EstimatedCharsPerToken = 1 << 2

var (
	seedIndexBytes = []byte("minnow-subword-idx-v1::")
	seedSignBytes  = []byte("minnow-subword-sgn-v1::")
)

func Vector(input string, dim, minNgram, maxNgram int) ([]float32, error) {
	normalized := NormalizeInput(input)
	if normalized == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}
	vec := make([]float32, dim)
	var wordCount int
	visitTokens(normalized, func(tok string) {
		if _, skip := Stopwords[tok]; skip {
			return
		}
		AddWordVector(vec, tok, dim, minNgram, maxNgram)
		wordCount++
	})
	if wordCount == 0 {
		return nil, fmt.Errorf("input has no indexable tokens after stopword filtering")
	}
	scale := float32(1) / float32(wordCount)
	for i := range vec {
		vec[i] *= scale
	}
	if !NormalizeVector(vec) {
		return nil, fmt.Errorf("failed to build embedding: zero vector")
	}
	return vec, nil
}

func AddWordVector(vec []float32, word string, dim, minNgram, maxNgram int) {
	bounded := "<" + word + ">"
	runes := []rune(bounded)
	addFeature(vec, stableHashString(seedIndexBytes, bounded), stableHashString(seedSignBytes, bounded), dim)
	for n := minNgram; n <= maxNgram && n <= len(runes); n++ {
		for i := 0; i <= len(runes)-n; i++ {
			addFeature(
				vec,
				stableHashRunes(seedIndexBytes, runes[i:i+n]),
				stableHashRunes(seedSignBytes, runes[i:i+n]),
				dim,
			)
		}
	}
}

func addFeature(vec []float32, indexHash, signHash uint64, dim int) {
	idx := int(indexHash % uint64(dim))
	if signHash%2 == 1 {
		vec[idx] -= float32(1)
	} else {
		vec[idx] += float32(1)
	}
}

func NormalizeInput(input string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(input))), " ")
}

func Tokenize(input string) []string {
	tokens := make([]string, 0, len(input)/EstimatedCharsPerToken)
	visitTokens(input, func(tok string) {
		tokens = append(tokens, tok)
	})
	return tokens
}

func visitTokens(input string, visit func(string)) int {
	count := 0
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		visit(b.String())
		count++
		b.Reset()
	}
	for _, r := range input {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return count
}

const (
	fnv64Offset uint64 = 14695981039346656037
	fnv64Prime  uint64 = 1099511628211

	utf8Rune1Max           rune = 0x80
	utf8Rune2Max           rune = 0x800
	utf8Rune3Max           rune = 0x10000
	utf8ContinuationMask   rune = 0x3f
	utf8ContinuationPrefix byte = 0x80
)

func stableHashString(seedWithSep []byte, token string) uint64 {
	h := fnv64Offset
	for _, b := range seedWithSep {
		h ^= uint64(b)
		h *= fnv64Prime
	}
	for i := 0; i < len(token); i++ {
		h ^= uint64(token[i])
		h *= fnv64Prime
	}
	return h
}

func stableHashRunes(seedWithSep []byte, runes []rune) uint64 {
	h := fnv64Offset
	for _, b := range seedWithSep {
		h ^= uint64(b)
		h *= fnv64Prime
	}
	for _, r := range runes {
		h = appendHashRune(h, r)
	}
	return h
}

func appendHashRune(h uint64, r rune) uint64 {
	switch {
	case r < utf8Rune1Max:
		return hashByte(h, byte(r))
	case r < utf8Rune2Max:
		h = hashByte(h, byte(0xc0|r>>6))
		return hashByte(h, byte(rune(utf8ContinuationPrefix)|r&utf8ContinuationMask))
	case r < utf8Rune3Max:
		h = hashByte(h, byte(0xe0|r>>12))
		h = hashByte(h, byte(rune(utf8ContinuationPrefix)|r>>6&utf8ContinuationMask))
		return hashByte(h, byte(rune(utf8ContinuationPrefix)|r&utf8ContinuationMask))
	default:
		h = hashByte(h, byte(0xf0|r>>18))
		h = hashByte(h, byte(rune(utf8ContinuationPrefix)|r>>12&utf8ContinuationMask))
		h = hashByte(h, byte(rune(utf8ContinuationPrefix)|r>>6&utf8ContinuationMask))
		return hashByte(h, byte(rune(utf8ContinuationPrefix)|r&utf8ContinuationMask))
	}
}

func hashByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= fnv64Prime
	return h
}

func NormalizeVector(vec []float32) bool {
	sumSq := 0.0
	for _, v := range vec {
		fv := float64(v)
		sumSq += fv * fv
	}
	if sumSq == 0 {
		return false
	}
	norm := float32(math.Sqrt(sumSq))
	for i := range vec {
		vec[i] /= norm
	}
	return true
}

var Stopwords = map[string]struct{}{
	"a": {}, "about": {}, "above": {}, "after": {}, "again": {}, "against": {}, "all": {}, "am": {}, "an": {}, "and": {},
	"any": {}, "are": {}, "as": {}, "at": {}, "be": {}, "because": {}, "been": {}, "before": {}, "being": {}, "below": {},
	"between": {}, "both": {}, "but": {}, "by": {}, "can": {}, "did": {}, "do": {}, "does": {}, "doing": {}, "down": {},
	"during": {}, "each": {}, "few": {}, "for": {}, "from": {}, "further": {}, "had": {}, "has": {}, "have": {}, "having": {},
	"he": {}, "her": {}, "here": {}, "hers": {}, "herself": {}, "him": {}, "himself": {}, "his": {}, "how": {}, "i": {},
	"if": {}, "in": {}, "into": {}, "is": {}, "it": {}, "its": {}, "itself": {}, "just": {}, "me": {}, "more": {},
	"most": {}, "my": {}, "myself": {}, "no": {}, "nor": {}, "not": {}, "now": {}, "of": {}, "off": {}, "on": {},
	"once": {}, "only": {}, "or": {}, "other": {}, "our": {}, "ours": {}, "ourselves": {}, "out": {}, "over": {}, "own": {},
	"same": {}, "she": {}, "should": {}, "so": {}, "some": {}, "such": {}, "than": {}, "that": {}, "the": {}, "their": {},
	"theirs": {}, "them": {}, "themselves": {}, "then": {}, "there": {}, "these": {}, "they": {}, "this": {}, "those": {},
	"through": {}, "to": {}, "too": {}, "under": {}, "until": {}, "up": {}, "very": {}, "was": {}, "we": {}, "were": {},
	"what": {}, "when": {}, "where": {}, "which": {}, "while": {}, "who": {}, "whom": {}, "why": {}, "with": {}, "you": {},
	"your": {}, "yours": {}, "yourself": {}, "yourselves": {},
}
