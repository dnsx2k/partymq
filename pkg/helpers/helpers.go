package helpers

import (
	"fmt"
)

func QueueFromHostname(hostname string) string {
	return fmt.Sprintf("partymq.q.partition-%s", hostname)
}
