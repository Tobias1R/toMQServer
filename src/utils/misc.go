package utils

import (
	"crypto/sha1"
	"encoding/base64"
)

func HashDigestString(str string) string {
	hasher := sha1.New()
	hasher.Write([]byte(str))
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	return sha
}

func RemoveStringFromSlice(slice []string, str string) []string {
	for i, v := range slice {
		if v == str {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
