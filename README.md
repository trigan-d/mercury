Agora Mercury. Pluggable module for SNS-SQS messaging pipeline.

TODOs:
* core-v2 and core-v1 Mercury adapters for easier configuration, thrift integration etc...
* core-v1 samples
* create mercury-common module for DTOs and other stuff
* use DLQ for unprocessed messages
* discuss and implement an ability to register several consumers for one topic
* handle messages that failed to delete
* wrap SNS and SQS interactions with hystrix commands
* create Mercury-Monitor service with UI for current topics and queues lists, manual message publishing/replay, etc.
* add some unit tests and integration tests

