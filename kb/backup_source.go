package kb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// confirmPinnedSource is the second half of the pin/delete handshake. Call it
// AFTER a provisional pin is durable: DeleteKnowledgeBase removes the source
// manifest BEFORE scanning pins and deleting bytes. Either deletion observes our pin,
// or this read observes its removed/changed source and the creator must abort.
// Checking the version also fences deletion followed by reuse of the KB ID.
func (l *KB) confirmPinnedSource(ctx context.Context, kbID, version string) error {
	current, err := l.ManifestStore.HeadVersion(ctx, kbID)
	if err != nil {
		return fmt.Errorf("confirm pinned source %q: %w", kbID, err)
	}
	if current == "" {
		return fmt.Errorf("confirm pinned source %q: %w", kbID, ErrManifestNotFound)
	}
	if current != version {
		return fmt.Errorf("%w: source manifest %q changed while pinning", ErrBlobVersionMismatch, kbID)
	}
	return nil
}

func (l *KB) publishManifestPin(ctx context.Context, kbID, version, key string, data []byte) error {
	pin, err := l.beginPublicationPin(ctx, kbID, data)
	if err != nil {
		return err
	}
	if err := l.confirmPinnedSource(ctx, kbID, version); err != nil {
		return pin.release(ctx, err)
	}
	// Only a source-confirmed descriptor enters the public backup/snapshot
	// namespace. Restorers must never consume an unconfirmed GC pin.
	if _, err := l.uploadBytesCreateOnly(ctx, key, data); err != nil {
		if errors.Is(err, ErrBackupExists) || errors.Is(err, ErrBlobVersionMismatch) {
			return pin.release(ctx, err)
		}
		return pin.retain(err) // The write may have committed despite its error.
	}
	return pin.release(ctx, nil)
}

func publicationPinPrefix(kbID string) string { return kbID + ".publication-pins/" }

type publicationPin struct {
	kb  *KB
	key string
}

// Provisional pins deliberately have no expiry: a timeout/crash can leave a
// committed public record behind. The returned error names the exact key to
// reconcile. After quiescing the failed publisher and verifying its destination
// and public marker, operators may remove that provisional key; branch public
// markers/ref owners are removed separately through DeleteBranch when unused.
// Never age these pins out on a timer or delete them by backup/branch ID.
func (l *KB) beginPublicationPin(ctx context.Context, kbID string, data []byte) (*publicationPin, error) {
	// A fresh, non-reused key is the cleanup identity. Never clean up a public
	// marker or shared ref-owner by logical ID: another attempt may replace it.
	pin := &publicationPin{kb: l, key: publicationPinPrefix(kbID) + rand.Text() + ".json"}
	if _, err := l.uploadBytesCreateOnly(ctx, pin.key, data); err != nil {
		return nil, pin.retain(err)
	}
	return pin, nil
}

// Provisional pins are scanned BEFORE public markers. This ordering is essential
// when moving protection from the provisional namespace to the public one: a
// deleter must see at least one side of that handoff.
func (p *publicationPin) release(ctx context.Context, cause error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := p.kb.BlobStore.Delete(cleanup, p.key); err != nil {
		return errors.Join(cause, fmt.Errorf("release provisional pin %s: %w", p.key, err))
	}
	return cause
}

func (p *publicationPin) retain(cause error) error {
	return fmt.Errorf("publication outcome requires reconciliation; provisional pin retained at %s: %w", p.key, cause)
}
