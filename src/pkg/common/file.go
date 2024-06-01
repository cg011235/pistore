package common

import (
	"os"
	"sync"
)

type File struct {
	POID         string
	Path         string
	Stats        os.FileInfo
	ChunkSize    uint32
	FingerPrints []string
	mu           sync.Mutex
}
