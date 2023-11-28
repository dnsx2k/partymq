package scenarios

import (
	"fmt"
	"time"

	"github.com/dnsx2k/partymq/tests/pkg/utils"
	amqp "github.com/rabbitmq/amqp091-go"
)

var partitions = []string{"simple-partition-0", "simple-partition-1", "simple-partition-2"}

var Simple = utils.Scenario{
	ExecuteScenarioFn: func(channel *amqp.Channel) {
		for i := range partitions {
			utils.CreateQueue(channel, partitions[i])

			client := fmt.Sprintf("simple-%v", i)
			key := utils.BindClient(client)
			utils.BindQueue(channel, partitions[i], key, "partymq.ex.write")

			utils.ClientReady(client)
			time.Sleep(5 * time.Second)
		}

		for {
			time.Sleep(3 * time.Second)
			n := utils.InspectNumberOfMessages(channel, "partymq.q.source")
			if n == 0 {
				break
			}
		}

		for i := 0; i < 3; i++ {
			client := fmt.Sprintf("simple-%v", i)
			utils.UnbindClient(client)
		}
	},
	MessageVolume: 100_000,
	IdPoolSize:    25_000,
	Partitions:    partitions,
}
