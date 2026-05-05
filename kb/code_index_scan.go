package kb

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

func scanCodebase(ctx context.Context, root string, opts CodeIndexOptions) ([]codeScannedFile, int, error) {
	paths, err := codebaseCandidatePaths(ctx, root, opts.IncludeUntracked)
	if err != nil {
		return nil, 0, err
	}
	files := make([]codeScannedFile, 0, len(paths))
	skipped := 0
	for _, rel := range paths {
		file, ok, err := scanCodebaseFile(ctx, root, rel, opts)
		if err != nil {
			return nil, skipped, err
		}
		if !ok {
			skipped++
			continue
		}
		files = append(files, file)
	}
	return files, skipped, nil
}

func codebaseCandidatePaths(ctx context.Context, root string, includeUntracked bool) ([]string, error) {
	paths, gitOK := gitCodePaths(ctx, root, includeUntracked)
	if !gitOK {
		var err error
		paths, err = walkCodePaths(ctx, root)
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func scanCodebaseFile(ctx context.Context, root, rel string, opts CodeIndexOptions) (codeScannedFile, bool, error) {
	if err := ctx.Err(); err != nil {
		return codeScannedFile{}, false, err
	}
	rel = filepath.ToSlash(filepath.Clean(rel))
	if !isEligibleCodeRelPath(rel, opts) {
		return codeScannedFile{}, false, nil
	}
	abs := filepath.Join(root, filepath.FromSlash(rel))
	info, err := os.Stat(abs)
	if err != nil || info.IsDir() || info.Size() <= 0 || info.Size() > opts.MaxFileBytes {
		return codeScannedFile{}, false, nil
	}
	binary, err := isLikelyBinaryContent(abs)
	if err != nil {
		return codeScannedFile{}, false, err
	}
	if binary {
		return codeScannedFile{}, false, nil
	}
	hash, err := FileContentSHA256(ctx, abs)
	if err != nil {
		return codeScannedFile{}, false, err
	}
	return codeScannedFile{AbsPath: abs, RelPath: rel, Hash: hash, SizeBytes: info.Size(), Language: detectCodeLanguage(rel)}, true, nil
}

func isEligibleCodeRelPath(rel string, opts CodeIndexOptions) bool {
	if rel == "." || strings.HasPrefix(rel, "../") || filepath.IsAbs(rel) {
		return false
	}
	if !matchesAnyCodePattern(rel, opts.Include) || matchesAnyCodePattern(rel, opts.Exclude) {
		return false
	}
	return !isLikelySecretPath(rel)
}

func gitCodePaths(ctx context.Context, root string, includeUntracked bool) ([]string, bool) {
	args := []string{"-C", root, "ls-files", "-c", "--exclude-standard"}
	if includeUntracked {
		args = []string{"-C", root, "ls-files", "-c", "-o", "--exclude-standard"}
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	out, err := cmd.Output()
	if err != nil {
		return nil, false
	}
	return splitLines(string(out)), true
}

func walkCodePaths(ctx context.Context, root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && matchesAnyCodePattern(rel+"/x", defaultCodeExcludePatterns) {
				return filepath.SkipDir
			}
			return nil
		}
		out = append(out, rel)
		return nil
	})
	return out, err
}

func isLikelyBinaryContent(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	buf := make([]byte, 8192)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return false, err
	}
	return isLikelyBinaryBytes(buf[:n]), nil
}

func isLikelyBinaryBytes(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	if bytes.Contains(data, []byte{0}) {
		return true
	}
	if !utf8.Valid(data) {
		return true
	}
	control := 0
	for _, b := range data {
		if isBinaryControlByte(b) {
			control++
		}
	}
	return float64(control)/float64(len(data)) > 0.30
}

func isBinaryControlByte(b byte) bool {
	return b < 0x20 && b != '\n' && b != '\r' && b != '\t' && b != '\f' && b != '\b'
}

func matchesAnyCodePattern(rel string, patterns []string) bool {
	if len(patterns) == 0 {
		return false
	}
	rel = filepath.ToSlash(rel)
	base := filepath.Base(rel)
	for _, raw := range patterns {
		if codePatternMatches(filepath.ToSlash(strings.TrimSpace(raw)), rel, base) {
			return true
		}
	}
	return false
}

func codePatternMatches(pattern string, rel string, base string) bool {
	if pattern == "" {
		return false
	}
	return codeDirectoryPatternMatches(pattern, rel) ||
		matchRecursiveCodePattern(pattern, rel) ||
		filepathPatternMatches(pattern, rel) ||
		basePatternMatches(pattern, base)
}

func codeDirectoryPatternMatches(pattern string, rel string) bool {
	if !strings.HasSuffix(pattern, "/**") {
		return false
	}
	prefix := strings.TrimSuffix(pattern, "/**")
	return rel == prefix || strings.HasPrefix(rel, prefix+"/")
}

func filepathPatternMatches(pattern string, value string) bool {
	ok, _ := filepath.Match(pattern, value)
	return ok
}

func basePatternMatches(pattern string, base string) bool {
	return !strings.Contains(pattern, "/") && filepathPatternMatches(pattern, base)
}

func matchRecursiveCodePattern(pattern, rel string) bool {
	re, err := regexp.Compile(codeGlobRegexp(pattern))
	if err != nil {
		return false
	}
	return re.MatchString(rel)
}

func codeGlobRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); {
		switch pattern[i] {
		case '*':
			if i+2 < len(pattern) && pattern[i:i+3] == "**/" {
				b.WriteString("(?:.*/)?")
				i += 3
				continue
			}
			if i+1 < len(pattern) && pattern[i:i+2] == "**" {
				b.WriteString(".*")
				i += 2
				continue
			}
			b.WriteString("[^/]*")
			i++
		case '?':
			b.WriteString("[^/]")
			i++
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
			i++
		}
	}
	b.WriteString("$")
	return b.String()
}

func isLikelySecretPath(rel string) bool {
	base := strings.ToLower(filepath.Base(rel))
	if base == ".env" || strings.HasPrefix(base, ".env.") || strings.HasSuffix(base, ".pem") || strings.HasSuffix(base, ".key") || strings.Contains(base, "credentials") || strings.Contains(base, "secret") {
		return true
	}
	return false
}

func splitLines(s string) []string {
	fields := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	out := fields[:0]
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}
