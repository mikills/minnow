package manifest

import "errors"

var (
	ErrNotFound     = errors.New("manifest not found")
	ErrInvalidStore = errors.New("invalid manifest store")
)
