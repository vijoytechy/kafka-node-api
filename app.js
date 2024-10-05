const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
const WebSocket = require('ws');
const app = express();
const port = process.env.PORT || 3000; // Use environment variable for the port

app.use(bodyParser.json());
app.use(cors());

function generateRandomTicketNumber() {
    const characters = '0123456789';
    let ticketNumber = '';
    for (let i = 0; i < 11; i++) {
        ticketNumber += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return 'TK' + ticketNumber;
}

app.post('/bookings', async (req, res) => {
    try {
        const bookingDetails = req.body;

        bookingDetails.status = 'Confirmed';
        bookingDetails.ticketNumber = generateRandomTicketNumber();

        await produceBookingUpdate(bookingDetails);

        res.status(200).json(bookingDetails);
    } catch (error) {
        console.error('Error processing booking:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

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
            { value: JSON.stringify(bookingDetails), key: bookingDetails.pnr },
        ],
    });

    await producer.disconnect();
    console.log("Disconnected from Kafka successfully");
}

const wss = new WebSocket.Server({ port: 8080 });
wss.on('connection', (ws) => {
    console.log('Client connected');

    ws.on('close', () => {
        console.log('Client disconnected');
    });
});

async function runConsumer() {
    let consumer;
    let stopped = false;

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
            fromBeginning: true,
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

            wss.clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(msg);
                }
            });
        },
    });

    while (!stopped) {
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    await consumer.disconnect();
}

runConsumer().catch((error) => console.error('ERROR TO START Kafka consumer', error));

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
