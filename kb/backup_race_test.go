package kb

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests force storage-boundary interleavings without sleeps, random
// scheduling, or a shared KB mutex/lease manager. They are correctness tests,
// not the seeded simulation suite.
func TestBackupRace(t *testing.T) {
	for _, operation := range []struct {
		name string
		pin  string
		run  func(context.Context, *KB) error
	}{
		{"branch", BranchRecordKey("src", "b1"), func(ctx context.Context, k *KB) error {
			_, err := k.BranchKB(ctx, "src", "b1", "dst")
			return err
		}},
		{"backup", BackupDescriptorKey("src", "b1"), func(ctx context.Context, k *KB) error {
			_, err := k.CreateBackup(ctx, "src", "b1")
			return err
		}},
		{"snapshot", SnapshotRecordKey("src", "b1"), func(ctx context.Context, k *KB) error {
			_, err := k.CreateSnapshot(ctx, "src", "b1")
			return err
		}},
	} {
		t.Run("delete_before_pin/"+operation.name, func(t *testing.T) {
			ctx := t.Context()
			creator, root := newOrphanKB(t)
			manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
			base := creator.BlobStore.(*LocalBlobStore)
			deleter := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
			fired := false
			creator.BlobStore = &backupRaceStore{LocalBlobStore: base, beforeCreate: func(ctx context.Context, key string) error {
				if strings.HasPrefix(key, publicationPinPrefix("src")) && !fired {
					fired = true
					// Source was already read. Delete wins through manifest
					// removal, the final pin scan, and physical shard deletion.
					return deleter.DeleteKnowledgeBase(ctx, "src")
				}
				return nil
			}}
			err := operation.run(ctx, creator)
			require.True(t, fired)
			require.ErrorIs(t, err, ErrManifestNotFound, "a stale source must not become a successful dangling pin")
			_, err = base.Head(ctx, operation.pin)
			require.ErrorIs(t, err, os.ErrNotExist, "loser must remove its marker")
			_, err = creator.ManifestStore.Get(ctx, "dst")
			require.ErrorIs(t, err, ErrManifestNotFound, "loser must not publish a destination")
			require.Empty(t, creator.refOwnersOf(ctx, manifest.Shards[0].Key), "loser must not leak ref-table owners")
		})

		t.Run("pin_after_guard/"+operation.name, func(t *testing.T) {
			ctx := t.Context()
			creator, root := newOrphanKB(t)
			manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
			base := creator.BlobStore.(*LocalBlobStore)
			deleter := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
			fired := false
			deleter.BlobStore = &backupRaceStore{LocalBlobStore: deleter.BlobStore.(*LocalBlobStore), beforeCreate: func(ctx context.Context, key string) error {
				if strings.HasPrefix(key, deletionMarkerPrefix("src")) && !fired {
					fired = true
					// Delete's initial guard has already reported no markers.
					// The new pin wins before source removal and must survive.
					return operation.run(ctx, creator)
				}
				return nil
			}}
			require.NoError(t, deleter.DeleteKnowledgeBase(ctx, "src"))
			require.True(t, fired)
			_, err := creator.ManifestStore.Get(ctx, "src")
			require.ErrorIs(t, err, ErrManifestNotFound)
			_, err = base.Head(ctx, operation.pin)
			require.NoError(t, err)
			for _, shard := range manifest.Shards {
				raw, err := base.DownloadBytes(ctx, shard.Key)
				require.NoError(t, err, "winning pin must retain intact bytes")
				require.Equal(t, "source bytes", string(raw))
			}
			if operation.name == "branch" {
				dst, err := creator.ManifestStore.Get(ctx, "dst")
				require.NoError(t, err)
				require.Equal(t, manifest.Shards, dst.Manifest.Shards)
			}
		})

		for _, failure := range []string{"changed", "cancelled", "unavailable"} {
			t.Run("source_"+failure+"/"+operation.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				creator, _ := newOrphanKB(t)
				manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
				base := creator.BlobStore.(*LocalBlobStore)
				want := errors.New("source HEAD unavailable")
				creator.ManifestStore = &backupRaceManifest{ManifestStore: creator.ManifestStore, sourceHead: func(context.Context) (string, error) {
					switch failure {
					case "changed":
						return "changed-source-version", nil
					case "cancelled":
						cancel()
						return "", context.Canceled
					default:
						return "", want
					}
				}}
				err := operation.run(ctx, creator)
				switch failure {
				case "changed":
					require.ErrorIs(t, err, ErrBlobVersionMismatch)
				case "cancelled":
					require.ErrorIs(t, err, context.Canceled)
				default:
					require.ErrorIs(t, err, want)
				}
				// Rollback uses a bounded, cancellation-independent context.
				_, err = base.Head(t.Context(), operation.pin)
				require.ErrorIs(t, err, os.ErrNotExist)
				require.Empty(t, creator.refOwnersOf(t.Context(), manifest.Shards[0].Key))
				raw, err := base.DownloadBytes(t.Context(), manifest.Shards[0].Key)
				require.NoError(t, err)
				require.Equal(t, "source bytes", string(raw), "rollback must not delete source bytes")
				_, err = creator.ManifestStore.Get(t.Context(), "dst")
				require.ErrorIs(t, err, ErrManifestNotFound)
			})
		}

		t.Run("stale_cleanup/"+operation.name, func(t *testing.T) {
			ctx := t.Context()
			first, root := newOrphanKB(t)
			manifest := branchSeed(t, first, "src", map[string]string{"s0": "source bytes"})
			second := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
			confirmErr := errors.New("confirmation unavailable")
			first.ManifestStore = &backupRaceManifest{ManifestStore: first.ManifestStore, sourceHead: func(ctx context.Context) (string, error) {
				switch operation.name {
				case "backup":
					require.NoError(t, second.DeleteBackup(ctx, "src", "b1"))
				case "snapshot":
					require.NoError(t, second.DeleteSnapshot(ctx, "src", "b1"))
				case "branch":
					require.NoError(t, second.DeleteBranch(ctx, "src", "b1"))
				}
				require.NoError(t, operation.run(ctx, second), "replacement must commit before stale cleanup")
				return "", confirmErr
			}}
			require.ErrorIs(t, operation.run(ctx, first), confirmErr)
			_, err := second.BlobStore.Head(ctx, operation.pin)
			require.NoError(t, err, "stale cleanup must not remove a replacement's public pin")
			require.ErrorIs(t, second.DeleteKnowledgeBase(ctx, "src"), ErrDeleteBlockedByBackups)
			raw, err := second.BlobStore.DownloadBytes(ctx, manifest.Shards[0].Key)
			require.NoError(t, err)
			require.Equal(t, "source bytes", string(raw))
			if operation.name == "branch" {
				require.NotEmpty(t, second.refOwnersOf(ctx, manifest.Shards[0].Key))
				_, err = second.ManifestStore.Get(ctx, "dst")
				require.NoError(t, err)
			}
		})
	}

	t.Run("restore_delete_before_pin", func(t *testing.T) {
		ctx := t.Context()
		creator, root := newOrphanKB(t)
		manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
		_, err := creator.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		base := creator.BlobStore.(*LocalBlobStore)
		deleter := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
		fired := false
		creator.BlobStore = &backupRaceStore{LocalBlobStore: base, beforeCreate: func(ctx context.Context, key string) error {
			if strings.HasPrefix(key, publicationPinPrefix("src")) && !fired {
				fired = true
				if err := deleter.DeleteBackup(ctx, "src", "b1"); err != nil {
					return err
				}
				return deleter.DeleteKnowledgeBase(ctx, "src")
			}
			return nil
		}}
		_, err = creator.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.True(t, fired)
		require.ErrorIs(t, err, ErrBackupNotFound)
		_, err = creator.GetBranch(ctx, "src", "r1")
		require.ErrorIs(t, err, ErrBackupNotFound)
		_, err = creator.ManifestStore.Get(ctx, "dst")
		require.ErrorIs(t, err, ErrManifestNotFound)
		require.Empty(t, creator.refOwnersOf(ctx, manifest.Shards[0].Key))
	})

	t.Run("restore_without_live_manifest", func(t *testing.T) {
		ctx := t.Context()
		creator, _ := newOrphanKB(t)
		manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
		_, err := creator.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		require.NoError(t, creator.ManifestStore.Delete(ctx, "src"))
		_, err = creator.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.NoError(t, err, "an intact backup, not a live manifest, authorizes restore")
		dst, err := creator.ManifestStore.Get(ctx, "dst")
		require.NoError(t, err)
		require.Equal(t, manifest.Shards, dst.Manifest.Shards)
	})

	t.Run("restore_during_delete", func(t *testing.T) {
		ctx := t.Context()
		loader, _ := newOrphanKB(t)
		branchSeed(t, loader, "src", map[string]string{"s0": "source bytes"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		// State at the physical-delete boundary when backup creation landed
		// after the initial guard: descriptor and bytes exist, source is gone,
		// and the deleter may already have captured an empty reachability set.
		require.NoError(t, loader.TombstoneKnowledgeBase(ctx, "src", "delete"))
		require.NoError(t, loader.ManifestStore.Delete(ctx, "src"))
		_, err = loader.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
		require.ErrorIs(t, err, ErrKBTombstoned, "a late restore must not cross an active deletion's final scan")
		_, err = loader.ManifestStore.Get(ctx, "dst")
		require.ErrorIs(t, err, ErrManifestNotFound)
		pins, err := loader.BlobStore.List(ctx, publicationPinPrefix("src"))
		require.NoError(t, err)
		require.Empty(t, pins)
	})

	t.Run("delete_requires_tombstone", func(t *testing.T) {
		ctx := t.Context()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "source bytes"})
		failure := errors.New("tombstone write unavailable")
		loader.BlobStore = &backupRaceStore{LocalBlobStore: loader.BlobStore.(*LocalBlobStore), beforeCreate: func(ctx context.Context, key string) error {
			if strings.HasPrefix(key, deletionMarkerPrefix("src")) {
				return failure
			}
			return nil
		}}
		require.ErrorIs(t, loader.DeleteKnowledgeBase(ctx, "src"), failure)
		_, err := loader.ManifestStore.Get(ctx, "src")
		require.NoError(t, err, "without a durable deletion marker, source must remain published")
		raw, err := loader.BlobStore.DownloadBytes(ctx, manifest.Shards[0].Key)
		require.NoError(t, err)
		require.Equal(t, "source bytes", string(raw))
	})

	t.Run("concurrent_delete_ownership", func(t *testing.T) {
		ctx := t.Context()
		first, root := newOrphanKB(t)
		manifest := branchSeed(t, first, "src", map[string]string{"s0": "source bytes"})
		second := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
		atDelete, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		first.BlobStore = &backupRaceStore{LocalBlobStore: first.BlobStore.(*LocalBlobStore), beforeDelete: func(ctx context.Context, key string) error {
			if key == manifest.Shards[0].Key {
				close(atDelete)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
		done := make(chan error, 1)
		go func() { done <- first.DeleteKnowledgeBase(ctx, "src") }()
		select {
		case <-atDelete:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// Source manifest and pin scan are gone; first still has physical work
		// pending. Second has no manifest to clean and finishes immediately.
		require.NoError(t, second.DeleteKnowledgeBase(ctx, "src"))
		active, err := second.IsTombstoned(ctx, "src")
		require.NoError(t, err)
		require.True(t, active, "second deletion must not clear the first's restore fence")
		markers, err := second.BlobStore.List(ctx, deletionMarkerPrefix("src"))
		require.NoError(t, err)
		require.Len(t, markers, 1)
		unblock()
		require.NoError(t, <-done)
		active, err = second.IsTombstoned(ctx, "src")
		require.NoError(t, err)
		require.False(t, active, "only the last owner can make the source unfenced")
	})

	t.Run("rollback_reports_failure", func(t *testing.T) {
		ctx := t.Context()
		creator, _ := newOrphanKB(t)
		manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
		sourceErr, cleanupErr := errors.New("source HEAD unavailable"), errors.New("pin delete unavailable")
		creator.ManifestStore = &backupRaceManifest{ManifestStore: creator.ManifestStore, sourceHead: func(context.Context) (string, error) {
			return "", sourceErr
		}}
		creator.BlobStore = &backupRaceStore{LocalBlobStore: creator.BlobStore.(*LocalBlobStore), beforeDelete: func(ctx context.Context, key string) error {
			if strings.HasPrefix(key, publicationPinPrefix("src")) {
				return cleanupErr
			}
			return nil
		}}
		_, err := creator.BranchKB(ctx, "src", "b1", "dst")
		require.ErrorIs(t, err, sourceErr)
		require.ErrorIs(t, err, cleanupErr)
		require.Empty(t, creator.refOwnersOf(ctx, manifest.Shards[0].Key))
		_, err = creator.ManifestStore.Get(ctx, "dst")
		require.ErrorIs(t, err, ErrManifestNotFound)
		_, err = creator.BlobStore.Head(ctx, manifest.Shards[0].Key)
		require.NoError(t, err)
	})

	t.Run("provisional_not_restorable", func(t *testing.T) {
		ctx := t.Context()
		creator, root := newOrphanKB(t)
		manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
		base := creator.BlobStore.(*LocalBlobStore)
		restorer := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
		deleter := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
		fired := false
		creator.BlobStore = &backupRaceStore{LocalBlobStore: base, create: func(ctx context.Context, key string, data []byte) (*BlobObjectInfo, error) {
			require.True(t, strings.HasPrefix(key, publicationPinPrefix("src")))
			var published *BlobObjectInfo
			deleter.BlobStore = &backupRaceStore{LocalBlobStore: deleter.BlobStore.(*LocalBlobStore), beforeDelete: func(ctx context.Context, deleted string) error {
				if deleted != manifest.Shards[0].Key {
					return nil
				}
				fired = true // Manifest and the final empty pin scan are complete.
				writer := &backupRaceStore{LocalBlobStore: base}
				var err error
				published, err = writer.UploadBytesIfNotExists(ctx, key, data)
				require.NoError(t, err)
				_, err = restorer.GetBackup(ctx, "src", "b1")
				require.ErrorIs(t, err, ErrBackupNotFound)
				_, err = restorer.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
				require.ErrorIs(t, err, ErrBackupNotFound, "a restorer must not consume a doomed provisional pin")
				return nil
			}}
			if err := deleter.DeleteKnowledgeBase(ctx, "src"); err != nil {
				return nil, err
			}
			return published, nil
		}}
		_, err := creator.CreateBackup(ctx, "src", "b1")
		require.True(t, fired)
		require.ErrorIs(t, err, ErrManifestNotFound)
		_, err = restorer.ManifestStore.Get(ctx, "dst")
		require.ErrorIs(t, err, ErrManifestNotFound)
		pins, err := base.List(ctx, publicationPinPrefix("src"))
		require.NoError(t, err)
		require.Empty(t, pins)
	})

	t.Run("promotion_scan_order", func(t *testing.T) {
		ctx := t.Context()
		loader, _ := newOrphanKB(t)
		manifest := branchSeed(t, loader, "src", map[string]string{"s0": "source bytes"})
		_, err := loader.CreateBackup(ctx, "src", "b1")
		require.NoError(t, err)
		base := loader.BlobStore.(*LocalBlobStore)
		data, err := base.DownloadBytes(ctx, BackupDescriptorKey("src", "b1"))
		require.NoError(t, err)
		require.NoError(t, loader.DeleteBackup(ctx, "src", "b1"))
		pin, err := loader.beginPublicationPin(ctx, "src", data)
		require.NoError(t, err)
		fired := false
		loader.BlobStore = &backupRaceStore{LocalBlobStore: base, beforeDownload: func(ctx context.Context, key string) error {
			if key == pin.key && !fired {
				fired = true
				// Complete promotion between List(pending) and Read(pending).
				// Public listing MUST follow the pending read, not precede it.
				_, err := base.UploadBytesIfMatch(ctx, BackupDescriptorKey("src", "b1"), data, "")
				if err != nil {
					return err
				}
				return base.Delete(ctx, pin.key)
			}
			return nil
		}}
		pinned, err := loader.backupPinnedShardKeys(ctx, "src")
		require.NoError(t, err)
		require.True(t, fired)
		require.Contains(t, pinned, manifest.Shards[0].Key)
	})

	for _, operation := range []string{"branch", "restore", "backup", "snapshot"} {
		for _, committed := range []bool{false, true} {
			name := "not_committed"
			if committed {
				name = "committed"
			}
			t.Run("uncertain_publish/"+operation+"/"+name, func(t *testing.T) {
				ctx := t.Context()
				creator, root := newOrphanKB(t)
				manifest := branchSeed(t, creator, "src", map[string]string{"s0": "source bytes"})
				base := creator.BlobStore.(*LocalBlobStore)
				if operation == "restore" {
					_, err := creator.CreateBackup(ctx, "src", "b1")
					require.NoError(t, err)
				}
				if operation == "branch" || operation == "restore" {
					creator.ManifestStore = &backupRaceCommitError{ManifestStore: creator.ManifestStore, committed: committed}
				} else {
					creator.BlobStore = &backupRaceStore{LocalBlobStore: base, create: func(ctx context.Context, key string, data []byte) (*BlobObjectInfo, error) {
						writer := &backupRaceStore{LocalBlobStore: base}
						if strings.HasPrefix(key, publicationPinPrefix("src")) {
							return writer.UploadBytesIfNotExists(ctx, key, data)
						}
						if committed {
							_, err := writer.UploadBytesIfNotExists(ctx, key, data)
							require.NoError(t, err)
						}
						return nil, context.DeadlineExceeded
					}}
				}
				var err error
				switch operation {
				case "branch":
					_, err = creator.BranchKB(ctx, "src", "b1", "dst")
				case "restore":
					_, err = creator.RestoreBackupZeroCopy(ctx, "src", "b1", "dst", "r1")
				case "backup":
					_, err = creator.CreateBackup(ctx, "src", "b1")
				case "snapshot":
					_, err = creator.CreateSnapshot(ctx, "src", "b1")
				}
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorContains(t, err, "requires reconciliation")
				pins, err := base.List(ctx, publicationPinPrefix("src"))
				require.NoError(t, err)
				require.Len(t, pins, 1, "an uncertain outcome must retain its uniquely identified protection")
				deleter := NewKB(&LocalBlobStore{Root: root}, t.TempDir())
				require.ErrorIs(t, deleter.DeleteKnowledgeBase(ctx, "src"), ErrDeleteBlockedByBackups)
				raw, err := base.DownloadBytes(ctx, manifest.Shards[0].Key)
				require.NoError(t, err)
				require.Equal(t, "source bytes", string(raw))
				if committed && (operation == "branch" || operation == "restore") {
					_, err = deleter.ManifestStore.Get(ctx, "dst")
					require.NoError(t, err)
				}
			})
		}
	}
}

type backupRaceStore struct {
	*LocalBlobStore
	beforeCreate   func(context.Context, string) error
	beforePut      func(context.Context, string) error
	beforeDelete   func(context.Context, string) error
	beforeDownload func(context.Context, string) error
	create         func(context.Context, string, []byte) (*BlobObjectInfo, error)
}

func (s *backupRaceStore) UploadBytesIfNotExists(ctx context.Context, key string, data []byte) (*BlobObjectInfo, error) {
	if s.create != nil {
		return s.create(ctx, key, data)
	}
	if s.beforeCreate != nil {
		if err := s.beforeCreate(ctx, key); err != nil {
			return nil, err
		}
	}
	file, err := os.CreateTemp("", "minnow-backup-race-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return s.LocalBlobStore.UploadIfNotExists(ctx, key, file.Name())
}

func (s *backupRaceStore) UploadBytesIfMatch(ctx context.Context, key string, data []byte, expected string) (*BlobObjectInfo, error) {
	if s.beforePut != nil {
		if err := s.beforePut(ctx, key); err != nil {
			return nil, err
		}
	}
	return s.LocalBlobStore.UploadBytesIfMatch(ctx, key, data, expected)
}

func (s *backupRaceStore) Delete(ctx context.Context, key string) error {
	if s.beforeDelete != nil {
		if err := s.beforeDelete(ctx, key); err != nil {
			return err
		}
	}
	return s.LocalBlobStore.Delete(ctx, key)
}

func (s *backupRaceStore) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	if s.beforeDownload != nil {
		if err := s.beforeDownload(ctx, key); err != nil {
			return nil, err
		}
	}
	return s.LocalBlobStore.DownloadBytes(ctx, key)
}

type backupRaceManifest struct {
	ManifestStore
	sourceHead func(context.Context) (string, error)
}

func (m *backupRaceManifest) HeadVersion(ctx context.Context, kbID string) (string, error) {
	if kbID == "src" && m.sourceHead != nil {
		return m.sourceHead(ctx)
	}
	return m.ManifestStore.HeadVersion(ctx, kbID)
}

type backupRaceCommitError struct {
	ManifestStore
	committed bool
}

func (m *backupRaceCommitError) UpsertIfMatch(ctx context.Context, kbID string, next SnapshotShardManifest, expected string) (string, error) {
	if kbID == "dst" {
		if m.committed {
			if _, err := m.ManifestStore.UpsertIfMatch(ctx, kbID, next, expected); err != nil {
				return "", err
			}
		}
		return "", context.DeadlineExceeded
	}
	return m.ManifestStore.UpsertIfMatch(ctx, kbID, next, expected)
}
