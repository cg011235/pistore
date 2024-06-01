package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"log"
	"os"
	"path/filepath"
	"pistore/src/pkg/utils"
)

// processPath processes a single file path, generating a unique identifier
func processPath(uniqueHostID string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Printf("Error converting to absolute path: %v", err)
			return err
		}

		hash := md5.Sum([]byte(uniqueHostID + absPath))
		poID := hex.EncodeToString(hash[:])
		log.Println("Processing path: ", absPath, " with POID: ", poID)
		return nil
	}
}

// processRoot walks through the directory structure from the root path
func processRoot(root, uniqueHostID string) {
	err := filepath.Walk(root, processPath(uniqueHostID))
	if err != nil {
		log.Fatalf("Error walking the path %q: %v\n", root, err)
	}
}

func main() {
	root := flag.String("path", "", "path to backup")
	token := flag.String("token", "", "security token for authentication")
	flag.Parse()

	if *root == "" {
		log.Fatal("Required argument path is not provided")
	}

	if *token == "" {
		log.Fatal("Required argument token is not provided")
	}

	uniqueHostID, err := utils.GetUniqueID()
	if err != nil {
		log.Fatalf("Failed to get unique host ID: %v", err)
	}

	processRoot(*root, uniqueHostID)
}
