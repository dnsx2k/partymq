package helpers

import (
	"fmt"
)

// TODO: Create routing key standard, pattern
func BuildRoutingKey(hostname string) string {
	return fmt.Sprintf("partymq.partition-%s", hostname)
}
