package localembed

import (
	"fmt"
	"hash/fnv"
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
	tokens := Tokenize(normalized)
	vec := make([]float32, dim)
	var wordCount int
	for _, tok := range tokens {
		if _, skip := Stopwords[tok]; skip {
			continue
		}
		AddWordVector(vec, tok, dim, minNgram, maxNgram)
		wordCount++
	}
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
	addFeature(vec, bounded, dim)
	for n := minNgram; n <= maxNgram && n <= len(runes); n++ {
		for i := 0; i <= len(runes)-n; i++ {
			addFeature(vec, string(runes[i:i+n]), dim)
		}
	}
}

func addFeature(vec []float32, feature string, dim int) {
	idx := int(stableHash(seedIndexBytes, feature) % uint64(dim))
	if stableHash(seedSignBytes, feature)%2 == 1 {
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
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, b.String())
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
	return tokens
}

func stableHash(seedWithSep []byte, token string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(seedWithSep)
	_, _ = h.Write([]byte(token))
	return h.Sum64()
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
