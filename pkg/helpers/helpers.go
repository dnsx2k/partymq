package helpers

import (
	"fmt"
	"strings"
)

func QueueFromHostname(hostname string) string {
	return fmt.Sprintf("partymq.q.partition-%s", hostname)
}

func HostnameFromQueue(queue string) string {
	return strings.Split(queue, "-")[1]
}
