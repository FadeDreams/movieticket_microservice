import amqp, { Channel, Connection, Message } from "amqplib/callback_api";
import { Ticket } from "./models/ticket";

export class RabbitMQService {
  private rabbitHost: string;
  private defaultQueueName: string;
  private channel: Channel | null = null;

  constructor(rabbitHost: string = "amqp://localhost", defaultQueueName: string = "queue1") {
    this.rabbitHost = rabbitHost;
    this.defaultQueueName = defaultQueueName;
  }


  public startProducer(queueName: string = this.defaultQueueName): Promise<(data: any) => void> {
    console.log("Producer rabbitMQ: connecting");

    return new Promise((resolve, reject) => {
      const connectAndCreateChannel = (retryCount: number = 0) => {
        amqp.connect(this.rabbitHost, (err: any, connection: Connection) => {
          if (err) {
            if (retryCount < 3) {
              console.log(`Producer rabbitMQ: connection attempt ${retryCount + 1} failed. Retrying...`);
              setTimeout(() => connectAndCreateChannel(retryCount + 1), 3000); // Retry after 3 seconds
            } else {
              reject(err); // Max retries reached, reject with the error
            }
          } else {
            connection.createChannel((err: any, channel: Channel) => {
              if (err) {
                if (retryCount < 3) {
                  console.log(`Producer rabbitMQ: channel creation attempt ${retryCount + 1} failed. Retrying...`);
                  setTimeout(() => connectAndCreateChannel(retryCount + 1), 3000); // Retry after 3 seconds
                } else {
                  reject(err); // Max retries reached, reject with the error
                }
              } else {
                this.channel = channel;

                // Declare the queue
                this.channel.assertQueue(queueName, { durable: false });

                console.log("Producer rabbitMQ: connected");

                resolve((data: any) => {
                  if (!this.channel) {
                    reject(new Error("Channel not initialized"));
                  }

                  const msg = JSON.stringify(data);

                  // Ensure the queue is declared before sending the message
                  this.channel.assertQueue(queueName, { durable: false });

                  this.channel.sendToQueue(queueName, Buffer.from(msg));
                });
              }
            });
          }
        });
      };

      connectAndCreateChannel();
    });
  }


  public startConsumer(queueName: string = this.defaultQueueName) {
    console.log("Consumer rabbitMQ : connecting");

    amqp.connect(this.rabbitHost, (err: any, conn) => {
      if (err) {
        throw err;
      }

      conn.createChannel((err, channel) => {
        if (err) {
          throw err;
        }

        this.channel = channel;

        // Declare the queue
        this.channel.assertQueue(queueName, { durable: false });

        console.log("Consumer rabbitMQ : connected");
        this.consumeMessages(queueName);
      });
    });
  }

  private consumeMessages(queueName: string) {
    if (!this.channel) {
      console.warn("Channel not ready. Cannot consume messages.");
      return;
    }

    this.channel.consume(
      queueName,
      async (data: Message | null) => {
        if (data) {
          const msg = JSON.parse(data.content.toString());
          console.log(msg); // do your thing with the message
          if (queueName === "ticket:created") {
            await this.handleOrderCreated(msg);
          } else if (queueName === "ticket:updated") {
            await this.handleOrderUpdated(msg);
          }
        }
      },
      { noAck: true }
    );
  }

  private async handleOrderCreated(data: any) {
    const { id, title, price, userId, version } = data;

    try {
      // Check if a ticket with the given ID already exists
      const existingTicket = await Ticket.findById(id);

      if (existingTicket) {
        console.warn(`Ticket with ID ${id} already exists. Updating instead of creating.`);
        // Handle logic to update the existing ticket if needed
        existingTicket.set({ title, price, userId, version });
        await existingTicket.save();
        console.log("Ticket updated:", existingTicket);
      } else {
        // Create a new ticket with version information
        const ticket = Ticket.build({ id, title, price, userId, version });

        // Save the ticket to the database using optimistic concurrency control
        await ticket.save();
        console.log("Ticket created:", ticket);
      }
    } catch (error) {
      console.error("Error creating/updating ticket:", error);
    }
  }

  private async handleOrderUpdated(data: any) {
    const { ticketId, updatedFields, version } = data;

    try {
      // Find the ticket by ID and version
      const ticket = await Ticket.findOne({ _id: ticketId, version: version - 1 });

      if (ticket) {
        // Update ticket properties based on the received data
        Object.assign(ticket, updatedFields);

        // Increment the version and save the updated ticket to the database
        await ticket.save();

        console.log("Ticket updated:", ticket);
      } else {
        console.log("Ticket not found or version mismatch for update:", ticketId, version);
      }
    } catch (error) {
      console.error("Error updating ticket:", error);
    }
  }
}

