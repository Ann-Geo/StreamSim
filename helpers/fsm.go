package helpers

import (
	"os"
	"strings"
	"time"
)

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func AppendDurationsToFile(path string, durations []time.Duration) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var parts []string
	for _, d := range durations {
		parts = append(parts, d.String())
	}

	singleLine := strings.Join(parts, ",")
	_, err = f.Write([]byte(singleLine + "\n")) // optional: newline after each append
	return err
}
