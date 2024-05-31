package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
)

// getMACAddress retrieves the MAC address of the first non-loopback network interface
func getMACAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, interf := range interfaces {
		if interf.Flags&net.FlagLoopback == 0 && interf.HardwareAddr != nil {
			return interf.HardwareAddr.String(), nil
		}
	}

	return "", fmt.Errorf("no valid network interface found")
}

// getHostname retrieves the hostname of the machine
func getHostname() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return hostname, nil
}

// getUniqueID generates a unique identifier for the host by combining the hostname and MAC address
func getUniqueID() (string, error) {
	macAddress, err := getMACAddress()
	if err != nil {
		return "", err
	}

	hostname, err := getHostname()
	if err != nil {
		return "", err
	}

	uniqueString := fmt.Sprintf("%s-%s", hostname, macAddress)
	hash := md5.Sum([]byte(uniqueString))
	return hex.EncodeToString(hash[:]), nil
}

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

	uniqueHostID, err := getUniqueID()
	if err != nil {
		log.Fatalf("Failed to get unique host ID: %v", err)
	}

	processRoot(*root, uniqueHostID)
}
