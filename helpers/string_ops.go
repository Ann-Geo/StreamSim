package helpers

import "strings"

func UpdatePortInUrl(amqpUrl string, newPort string) string {
	// Split into two parts at the last colon (before the port)
	lastColon := strings.LastIndex(amqpUrl, ":")
	if lastColon == -1 {
		return amqpUrl // no colon found, return as is
	}

	// Split prefix and old port
	prefix := amqpUrl[:lastColon+1] // includes colon
	return prefix + newPort
}
