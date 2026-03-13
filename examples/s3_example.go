package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mikills/kbcore/kb"
	kbduckdb "github.com/mikills/kbcore/kb/duckdb"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// 1. First load downloads from S3 (slower)
// 2. Subsequent loads use local cache (fast)
// 3. Uploads use optimistic concurrency with ETags
// 4. Cache eviction removes local files but keeps S3 version
// 5. Multiple instances can share the same S3 bucket
func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	s3Client := s3.NewFromConfig(cfg)

	blobStore := kb.NewS3BlobStore(s3Client, "my-knowledge-bases", "prod/")

	knowledgeBase := kb.NewKB(blobStore, "/tmp/kb-cache")
	artifactFormat, err := kbduckdb.NewArtifactFormat(kbduckdb.NewDepsFromKB(knowledgeBase,
		kbduckdb.WithMemoryLimit("128MB"),
	))

	if err != nil {
		log.Fatal(err)
	}

	if err := knowledgeBase.RegisterFormat(artifactFormat); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully configured KB + DuckDB format for S3-backed storage")
}
