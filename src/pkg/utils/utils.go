package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net"
	"os"
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
func GetUniqueID() (string, error) {
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
