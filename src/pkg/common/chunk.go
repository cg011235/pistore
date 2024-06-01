package common

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

type Chunk struct {
	ChunkID          [16]byte `json:"chunk_id"`           // Checksum of the entire chunk file (used as ChunkID)
	BirthVersion     uint64   `json:"birth_version"`      // Incremental number indicating incremental chunk data
	POID             [16]byte `json:"poid"`               // Permanent Object ID of the original file
	Offset           uint64   `json:"offset"`             // Offset of the chunk in the original file
	TimeStamp        int64    `json:"timestamp"`          // Timestamp when the chunk was created (Unix timestamp)
	OriginalSize     uint64   `json:"original_size"`      // Original size of the chunk
	CompressedSize   uint64   `json:"compressed_size"`    // Compressed size of the chunk
	OriginalDataFP   [16]byte `json:"original_data_fp"`   // Checksum of the original data
	CompressedDataFP [16]byte `json:"compressed_data_fp"` // Checksum of the compressed data
	CompressionAlg   string   `json:"compression_alg"`    // Compression algorithm used (optional)
	Data             []byte   `json:"data"`               // The actual data of the chunk (compressed)
}

func computeMD5(data []byte) [16]byte {
	hash := md5.Sum(data)
	return hash
}

func compressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressData(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(data)
	reader, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

func NewChunkFromOriginalData(birthVersion uint64, poid [16]byte, offset uint64, originalData []byte) (*Chunk, error) {
	compressedData, err := compressData(originalData)
	if err != nil {
		return nil, err
	}

	originalDataFP := computeMD5(originalData)
	compressedDataFP := computeMD5(compressedData)

	chunk := &Chunk{
		BirthVersion:     birthVersion,
		POID:             poid,
		Offset:           offset,
		TimeStamp:        time.Now().Unix(),
		OriginalSize:     uint64(len(originalData)),
		CompressedSize:   uint64(len(compressedData)),
		OriginalDataFP:   originalDataFP,
		CompressedDataFP: compressedDataFP,
		CompressionAlg:   "gzip",
		Data:             compressedData,
	}

	serialized, _ := chunk.SerializeWithoutChunkID()
	chunk.ChunkID = computeMD5(serialized)
	return chunk, nil
}

func NewChunkFromCompressedData(birthVersion uint64, poid [16]byte, offset, originalSize, compressedSize uint64, originalData, compressedData []byte, compressionAlg string) *Chunk {
	originalDataFP := computeMD5(originalData)
	compressedDataFP := computeMD5(compressedData)
	chunk := &Chunk{
		BirthVersion:     birthVersion,
		POID:             poid,
		Offset:           offset,
		TimeStamp:        time.Now().Unix(),
		OriginalSize:     originalSize,
		CompressedSize:   compressedSize,
		OriginalDataFP:   originalDataFP,
		CompressedDataFP: compressedDataFP,
		CompressionAlg:   compressionAlg,
		Data:             compressedData,
	}
	serialized, _ := chunk.SerializeWithoutChunkID()
	chunk.ChunkID = computeMD5(serialized)
	return chunk
}

func (c *Chunk) SerializeWithoutChunkID() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, c.BirthVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.POID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.TimeStamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.OriginalSize); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.CompressedSize); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.OriginalDataFP); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.CompressedDataFP); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, c.CompressionAlg); err != nil {
		return nil, err
	}
	if _, err := buf.Write(c.Data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Chunk) Serialize() ([]byte, error) {
	dataWithoutChunkID, err := c.SerializeWithoutChunkID()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, c.ChunkID); err != nil {
		return nil, err
	}
	buf.Write(dataWithoutChunkID)
	return buf.Bytes(), nil
}

func DeserializeChunk(data []byte) (*Chunk, error) {
	buf := bytes.NewBuffer(data)
	var chunk Chunk
	if err := binary.Read(buf, binary.LittleEndian, &chunk.ChunkID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.BirthVersion); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.POID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.TimeStamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.OriginalSize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.CompressedSize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.OriginalDataFP); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.CompressedDataFP); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &chunk.CompressionAlg); err != nil {
		return nil, err
	}
	chunk.Data = make([]byte, chunk.CompressedSize)
	if _, err := buf.Read(chunk.Data); err != nil {
		return nil, err
	}
	return &chunk, nil
}

func (c *Chunk) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func (c *Chunk) FromJSON(data []byte) error {
	return json.Unmarshal(data, c)
}

// DumpToFile writes the chunk to a file
func (c *Chunk) DumpToFile() error {
	serialized, err := c.Serialize()
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%x.chunk", c.ChunkID)

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(serialized); err != nil {
		return err
	}

	return nil
}

// ReadFromFile reads a chunk from a file
func (c *Chunk) ReadFromFile(filename string) error {
	fileData, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	chunk, err := DeserializeChunk(fileData)
	if err != nil {
		return err
	}

	*c = *chunk
	return nil
}

func (c *Chunk) VerifyCompressedDataChecksum() bool {
	return c.CompressedDataFP == computeMD5(c.Data)
}

func (c *Chunk) VerifyOriginalDataChecksum() bool {
	originalData, _ := decompressData(c.Data)
	return c.OriginalDataFP == computeMD5(originalData)
}

func (c *Chunk) VerifyChunkID(data []byte) bool {
	return c.ChunkID == computeMD5(data[16:])
}
