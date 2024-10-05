const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors'); // Import CORS
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
const WebSocket = require('ws');
const app = express();
const port = 3000; // You can choose any available port

// Middleware to parse JSON requests
app.use(bodyParser.json());
app.use(cors()); // Enable CORS for all routes

// Function to generate a random ticket number
function generateRandomTicketNumber() {
    const characters = '0123456789';
    let ticketNumber = '';
    for (let i = 0; i < 11; i++) {
        ticketNumber += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return 'TK' + ticketNumber;  // Prefix with 'TK'
}

// POST endpoint to receive booking details
app.post('/bookings', async (req, res) => {
    try {
        const bookingDetails = req.body;

        // Modify the booking details
        bookingDetails.status = 'Confirmed';
        bookingDetails.ticketNumber = generateRandomTicketNumber();

        // Produce the updated booking details to Kafka
        await produceBookingUpdate(bookingDetails);

        // Respond with the updated booking details
        res.status(200).json(bookingDetails);
    } catch (error) {
        console.error('Error processing booking:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// Function to produce booking updates to Kafka
async function produceBookingUpdate(bookingDetails) {
    const kafka = new Kafka({
        kafkaJS: {
            brokers: [process.env.BOOTSTRAP_SERVERS],
            ssl: true,
            sasl: {
                mechanism: process.env.SASL_MECHANISMS,
                username: process.env.SASL_USERNAME,
                password: process.env.SASL_PASSWORD,
            },
        },
    });

    const producer = kafka.producer();

    await producer.connect();
    console.log("Connected to Kafka successfully");

    await producer.send({
        topic: 'bookings',
        messages: [
            { value: JSON.stringify(bookingDetails) , key: bookingDetails.pnr},
        ],
    });

    await producer.disconnect();
    console.log("Disconnected from Kafka successfully");
}

// Setup WebSocket server
const wss = new WebSocket.Server({ port: 8080 });
wss.on('connection', (ws) => {
    console.log('Client connected');

    ws.on('close', () => {
        console.log('Client disconnected');
    });
});

// Function to run the Kafka consumer and send messages to WebSocket clients
async function runConsumer() {
    let consumer;
    let stopped = false;

    // Initialization with fromBeginning set to true
    consumer = new Kafka({
        kafkaJS: {
            brokers: [process.env.BOOTSTRAP_SERVERS],
            ssl: true,
            sasl: {
                mechanism: process.env.SASL_MECHANISMS,
                username: process.env.SASL_USERNAME,
                password: process.env.SASL_PASSWORD,
            },
        },
    }).consumer({
        kafkaJS: {
            groupId: 'booking-consumer-group',
            fromBeginning: true, // Set to true to consume from the beginning
        },
    });

    await consumer.connect();
    console.log("Connected to Kafka Consumer");

    await consumer.subscribe({ topics: ['bookings'] });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const msg = message.value.toString();
            console.log({
                topic,
                partition,
                offset: message.offset,
                key: message.key?.toString(),
                value: msg,
            });

            // Send message to all connected WebSocket clients
            wss.clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(msg);
                }
            });
        },
    });

    // Keeping the consumer running until stopped
    while (!stopped) {
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    await consumer.disconnect();
}

// Start the Kafka consumer
runConsumer().catch((error) => console.error('ERROR TO START Kafka consumer', error));

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
