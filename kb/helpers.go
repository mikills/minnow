package kb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

func BuildInClausePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ",")
}

func MapKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func copyFileSync(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

func replaceFileWithCopy(src, dest string) error {
	tmpDest := fmt.Sprintf("%s.tmp-%d", dest, time.Now().UnixNano())
	if err := copyFileSync(src, tmpDest); err != nil {
		return err
	}
	defer os.Remove(tmpDest)

	if err := os.Rename(tmpDest, dest); err != nil {
		return err
	}

	return nil
}

func FileContentSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ExpandedFromVector(results []QueryResult) []ExpandedResult {
	expanded := make([]ExpandedResult, 0, len(results))
	for _, r := range results {
		sim := 1.0 / (1.0 + r.Distance)
		expanded = append(expanded, ExpandedResult{
			ID:         r.ID,
			Content:    r.Content,
			Distance:   r.Distance,
			GraphScore: 0,
			Score:      sim,
		})
	}
	return expanded
}

func TopNEntityScores(scores map[string]float64, n int) map[string]float64 {
	if n <= 0 || len(scores) <= n {
		return scores
	}
	type pair struct {
		id    string
		score float64
	}
	pairs := make([]pair, 0, len(scores))
	for id, score := range scores {
		pairs = append(pairs, pair{id: id, score: score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score == pairs[j].score {
			return pairs[i].id < pairs[j].id
		}
		return pairs[i].score > pairs[j].score
	})
	if len(pairs) > n {
		pairs = pairs[:n]
	}
	result := make(map[string]float64, len(pairs))
	for _, p := range pairs {
		result[p.id] = p.score
	}
	return result
}
