
### Fixed
- **rabbitmq**: `RabbitMQService` (`rabbitmq.ts`) no longer silently drops messages on processing failure. Consumer used `noAck: true`, so a `JSON.parse` error or any processing failure would still acknowledge (and lose) the message. Switched to explicit ack/nack (`noAck: false`), with failed messages routed through a dead-letter exchange (30s TTL backoff, max 5 attempts, tracked via `x-death` header) before landing in a terminal DLQ (`<queue>.dlq`) for triage — matching the pattern used in the order processor services. (`f965dd8`)

### Changed
- **rabbitmq**: Producer and consumer queues are now declared `durable: true` (previously `false`), and published messages are marked `persistent: true`, so queue definitions and in-flight messages survive a broker restart. ⚠️ Any existing `queue1` (or other queue used with this service) created under the old non-durable/no-DLX declaration must be deleted (once empty) before deploy, or RabbitMQ will throw `PRECONDITION_FAILED` on `assertQueue` due to mismatched arguments.
- **rabbitmq**: Migrated from `amqplib/callback_api` to the promise-based `amqplib` API. `startProducer` and `startConsumer` are now `async` — call sites must `await` them. (`f965dd8`)
