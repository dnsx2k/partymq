# [WIP] PartyMQ - dynamic partitioning with RabbitMQ

PartyMQ is an innovative backend application that extends the capabilities of the RabbitMQ message broker,
allowing for efficient horizontal scaling and concurrent processing of messages.
Designed to alleviate the challenges posed by processing a high volume of messages while avoiding data races,
PartyMQ introduces a robust partitioning system that intelligently divides messages into multiple partitions
for parallel processing.

***Horizontal Scalability:*** PartyMQ's core feature is its ability to horizontally scale message processing.
By breaking down the processing load into distinct partitions,
it enables seamless distribution of messages across multiple processing nodes.
This ensures optimal utilization of resources and eliminates the performance bottlenecks associated with traditional,
single-threaded message processing.

***Intelligent Partitioning:*** PartyMQ partitions messages based on a partition key,
which can be extracted from either message headers or the message body itself.
This key serves as the basis for distributing messages among different partitions.
As a result, messages with the same partition key are guaranteed to be processed within the same partition,
ensuring message order consistency.

***Data Race Prevention:*** Data races occur when multiple threads or processes access shared resources concurrently,
leading to unpredictable behavior and potential data corruption.
PartyMQ's partitioned architecture inherently avoids data races by isolating messages within their designated partitions,
thus enabling safe and synchronized parallel processing.

***Seamless Integration:*** As an extension of RabbitMQ, PartyMQ seamlessly integrates with existing RabbitMQ deployments.
It maintains compatibility with the familiar RabbitMQ interface,
making adoption straightforward and minimizing disruptions to existing workflows.

***Autoscaling Readiness:*** The flexibility of PartyMQ's partitioning system aligns seamlessly with auto-scaling strategies.
As the number of client pods scales up or down,
PartyMQ's architecture ensures that the newly added pods are integrated into the partitioning scheme,
without disrupting ongoing message processing.

## Client binding flow:

![client-binding.drawio.png](docs%2Fclient-binding.drawio.png)

1. Client sends POST request to PartyMQ API.

[clients-http-handler-source-code](cmd/clientshttphandler/http.go)

2. Server responds with json payload:
```json
{"routingKey": "partymq.partition-hostname01", "exchange":"partymq.ex.write"}
```

Routing key generation based on provided hostname:

[routing-key-generation-source-code](pkg/helpers/helpers.go)


3. Client declares queue and binds it to an exchange from json response.

4. Clients sends POST request to PartyMQ API to indicate that pod is ready to process messages.


## Message flow:

### Standard message flow:

![standard-msg-flow.drawio.png](docs%2Fstandard-msg-flow.drawio.png)

### Partitioned message flow:

![partitioned-msg-flow.drawio.png](docs%2Fpartitioned-msg-flow.drawio.png)