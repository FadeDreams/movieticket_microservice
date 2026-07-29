import amqp, { Channel, ChannelModel, ConsumeMessage } from "amqplib";

const DLX_NAME = "dlx.orders";
const DEAD_ROUTING_KEY = "orders.process.dead";
const RETRY_QUEUE_NAME = "orders.process.retry";
const DLQ_SUFFIX = ".dlq";
const RETRY_TTL_MS = 30_000; // 30s backoff
const MAX_RETRIES = 5;

export class RabbitMQService {
  private rabbitHost: string;
  private defaultQueueName: string;
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;

  constructor(
    rabbitHost: string = "amqp://localhost",
    defaultQueueName: string = "queue1"
  ) {
    this.rabbitHost = rabbitHost;
    this.defaultQueueName = defaultQueueName;
  }

  private getDeathCount(msg: ConsumeMessage, queueName: string): number {
    const deaths = (msg.properties.headers?.["x-death"] as any[]) || [];
    const entry = deaths.find((d) => d.queue === queueName);
    return entry ? Number(entry.count) : 0;
  }

  private async declareTopology(channel: Channel, queueName: string) {
    await channel.assertExchange(DLX_NAME, "direct", { durable: true });

    await channel.assertQueue(queueName, {
      durable: true,
      arguments: {
        "x-dead-letter-exchange": DLX_NAME,
        "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
      },
    });

    await channel.assertQueue(RETRY_QUEUE_NAME, {
      durable: true,
      arguments: {
        "x-message-ttl": RETRY_TTL_MS,
        "x-dead-letter-exchange": "", // default exchange
        "x-dead-letter-routing-key": queueName, // bounces back to the main queue after TTL
      },
    });
    await channel.bindQueue(RETRY_QUEUE_NAME, DLX_NAME, DEAD_ROUTING_KEY);

    const dlqName = `${queueName}${DLQ_SUFFIX}`;
    await channel.assertQueue(dlqName, { durable: true });

    return dlqName;
  }

  public async startProducer(queueName: string = this.defaultQueueName) {
    console.log("Producer rabbitMQ : connecting");
    this.connection = await amqp.connect(this.rabbitHost);
    const channel = await this.connection.createChannel();
    this.channel = channel;

    await this.declareTopology(channel, queueName);
    console.log("Producer rabbitMQ : connected");

    return (data: any) => {
      if (!this.channel) {
        throw new Error("Channel not initialized");
      }
      const msg = JSON.stringify(data);
      this.channel.sendToQueue(queueName, Buffer.from(msg), {
        persistent: true,
      });
    };
  }

  public async startConsumer(queueName: string = this.defaultQueueName) {
    console.log("Consumer rabbitMQ : connecting");
    this.connection = await amqp.connect(this.rabbitHost);
    const channel = await this.connection.createChannel();
    console.log("Consumer rabbitMQ : connected");

    const dlqName = await this.declareTopology(channel, queueName);
    await channel.prefetch(1);

    await channel.consume(
      queueName,
      async (data: ConsumeMessage | null) => {
        if (!data) return;

        const deathCount = this.getDeathCount(data, queueName);

        if (deathCount >= MAX_RETRIES) {
          // Exhausted retries — publish straight to the terminal DLQ
          // and ack it off the main queue so it stops cycling.
          console.error(
            `Message exceeded max retries (${MAX_RETRIES}), moving to ${dlqName}`
          );
          channel.sendToQueue(dlqName, data.content, {
            persistent: true,
            headers: data.properties.headers,
          });
          channel.ack(data);
          return;
        }

        try {
          const msg = JSON.parse(data.content.toString());
          console.log(msg); // do your thing with the message

          // TODO: if your processing logic can fail (e.g. a downstream
          // HTTP call), await it here and throw on failure so the
          // catch block below nacks it into the retry/DLQ path instead
          // of silently acking.

          channel.ack(data);
        } catch (err) {
          console.error("Error processing message:", err);
          // requeue=false -> routes through the DLX into the retry
          // queue (30s TTL backoff) instead of looping instantly.
          channel.nack(data, false, false);
        }
      },
      { noAck: false }
    );
  }

  public async close() {
    await this.channel?.close();
    await this.connection?.close();
  }
}
