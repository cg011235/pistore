package common

import (
	"fmt"
	"os"
	"testing"
)

// TestNewChunkFromOriginalData tests creating a new chunk from original data
func TestNewChunkFromOriginalData(t *testing.T) {
	originalData := []byte("This is the original chunk data")
	poid := [16]byte{}
	copy(poid[:], "example-poid")
	birthVersion := uint64(1)
	offset := uint64(0)

	chunk, err := NewChunkFromOriginalData(birthVersion, poid, offset, originalData)
	if err != nil {
		t.Fatalf("Failed to create new chunk: %v", err)
	}

	if chunk.OriginalSize != uint64(len(originalData)) {
		t.Errorf("Expected OriginalSize %d, got %d", len(originalData), chunk.OriginalSize)
	}

	compressedData, _ := compressData(originalData)
	if chunk.CompressedSize != uint64(len(compressedData)) {
		t.Errorf("Expected CompressedSize %d, got %d", len(compressedData), chunk.CompressedSize)
	}

	if !chunk.VerifyCompressedDataChecksum() {
		t.Errorf("Compressed data checksum verification failed")
	}

	if !chunk.VerifyOriginalDataChecksum() {
		t.Errorf("Original data checksum verification failed")
	}
}

// TestSerialization tests chunk serialization and deserialization
func TestSerialization(t *testing.T) {
	originalData := []byte("This is the original chunk data")
	poid := [16]byte{}
	copy(poid[:], "example-poid")
	birthVersion := uint64(1)
	offset := uint64(0)

	chunk, err := NewChunkFromOriginalData(birthVersion, poid, offset, originalData)
	if err != nil {
		t.Fatalf("Failed to create new chunk: %v", err)
	}

	serialized, err := chunk.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize chunk: %v", err)
	}

	deserializedChunk, err := DeserializeChunk(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize chunk: %v", err)
	}

	if deserializedChunk.OriginalSize != chunk.OriginalSize {
		t.Errorf("Expected OriginalSize %d, got %d", chunk.OriginalSize, deserializedChunk.OriginalSize)
	}

	if !deserializedChunk.VerifyCompressedDataChecksum() {
		t.Errorf("Compressed data checksum verification failed")
	}

	if !deserializedChunk.VerifyOriginalDataChecksum() {
		t.Errorf("Original data checksum verification failed")
	}
}

// TestJSONConversion tests JSON serialization and deserialization
func TestJSONConversion(t *testing.T) {
	originalData := []byte("This is the original chunk data")
	poid := [16]byte{}
	copy(poid[:], "example-poid")
	birthVersion := uint64(1)
	offset := uint64(0)

	chunk, err := NewChunkFromOriginalData(birthVersion, poid, offset, originalData)
	if err != nil {
		t.Fatalf("Failed to create new chunk: %v", err)
	}

	jsonData, err := chunk.ToJSON()
	if err != nil {
		t.Fatalf("Failed to serialize chunk to JSON: %v", err)
	}

	jsonChunk := &Chunk{}
	if err := jsonChunk.FromJSON(jsonData); err != nil {
		t.Fatalf("Failed to deserialize chunk from JSON: %v", err)
	}

	if jsonChunk.OriginalSize != chunk.OriginalSize {
		t.Errorf("Expected OriginalSize %d, got %d", chunk.OriginalSize, jsonChunk.OriginalSize)
	}

	if !jsonChunk.VerifyCompressedDataChecksum() {
		t.Errorf("Compressed data checksum verification failed")
	}

	if !jsonChunk.VerifyOriginalDataChecksum() {
		t.Errorf("Original data checksum verification failed")
	}
}

// TestFileOperations tests writing and reading chunk to and from a file
func TestFileOperations(t *testing.T) {
	originalData := []byte("This is the original chunk data")
	poid := [16]byte{}
	copy(poid[:], "example-poid")
	birthVersion := uint64(1)
	offset := uint64(0)

	chunk, err := NewChunkFromOriginalData(birthVersion, poid, offset, originalData)
	if err != nil {
		t.Fatalf("Failed to create new chunk: %v", err)
	}

	filename := fmt.Sprintf("%x.chunk", chunk.ChunkID)
	defer os.Remove(filename)

	if err := chunk.DumpToFile(); err != nil {
		t.Fatalf("Failed to dump chunk to file: %v", err)
	}

	readChunk := &Chunk{}
	if err := readChunk.ReadFromFile(filename); err != nil {
		t.Fatalf("Failed to read chunk from file: %v", err)
	}

	if readChunk.OriginalSize != chunk.OriginalSize {
		t.Errorf("Expected OriginalSize %d, got %d", chunk.OriginalSize, readChunk.OriginalSize)
	}

	if !readChunk.VerifyCompressedDataChecksum() {
		t.Errorf("Compressed data checksum verification failed")
	}

	if !readChunk.VerifyOriginalDataChecksum() {
		t.Errorf("Original data checksum verification failed")
	}
}
