require("dotenv").config();
const amqp = require("amqplib");

let connection;
let channel;

async function connectRabbit() {
  try {
    const {
      RABBITMQ_USER,
      RABBITMQ_PASS,
      RABBITMQ_HOST,
      RABBITMQ_PORT = 5672,
      RABBITMQ_VHOST = "/"
    } = process.env;

    if (!RABBITMQ_USER || !RABBITMQ_PASS || !RABBITMQ_HOST) {
      throw new Error("RabbitMQ env is incomplete");
    }

    const url = `amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}/${encodeURIComponent(
      RABBITMQ_VHOST
    )}`;

    console.log("🔌 Connecting to RabbitMQ:", RABBITMQ_HOST);

    connection = await amqp.connect(url);
    channel = await connection.createChannel();

    console.log("✅ RabbitMQ connected");
    return channel;
  } catch (err) {
    console.error("❌ RabbitMQ connection failed:", err.message);
    process.exit(1);
  }
}

function getChannel() {
  if (!channel) {
    throw new Error("RabbitMQ channel not initialized");
  }
  return channel;
}

module.exports = {
  connectRabbit,
  getChannel
};
