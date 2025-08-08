package helpers

import (
	"bytes"
	"math/rand"
	"time"
)

func RandomString(length int) string {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	rand.Seed(time.Now().UnixNano())
	var result bytes.Buffer
	for i := 0; i < length; i++ {
		result.WriteByte(letters[rand.Intn(len(letters))])
	}
	return result.String()
}

func RandomFileContent(length int) string {
	return RandomString(length)
}

func RandomImageText(length int) string {
	return RandomString(length)
}

func RandomBinaryData(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, length)
	rand.Read(data)
	return data
}

func RandomID() int64 {
	timestamp := time.Now().UnixNano() /// int64(time.Millisecond)
	DebugLogger.Log(DEBUG, "Timestamp generated for RunnerId: %v\n", timestamp)
	//uniqueID := fmt.Sprintf("id_%d", timestamp)
	//return uniqueID
	return timestamp
}
