// Añadir las dependencias
const express = require('express');
const bodyParser = require('body-parser');
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const app = express();

// Setear variables: puertos, nombre de base de datos,
// direccion de base de datos mongodb
const port = 3000;
const mongoURI = 'mongodb://mongodb:27017';
const dbName = 'myDatabase';

// Aceptamos solicitudes JSON
app.use(bodyParser.json());

// Inicializar el cliente de kafka
const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['kafka:9092'],
});

// Inicializar el producer y consumer
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'my-group' });

// Inicializar la instancia de mongoDb
const mongoClient = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });

// Funcion que consume y se suscribe a la cola de eventos
const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'kafka-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        console.log({
          value: message.value.toString(),
          offset: message.offset,
          partition,
        });
        // Guardamos la data en mongoDb
        await saveToMongo(message)
      } catch (error) {
        console.error('Error processing Kafka message:', error);
      }
    },
  });
};

consumeMessages();

// Endpoint que recibe solicitudes desde pyspark
app.post('/api/sendToKafka', async (req, res) => {
  console.log("api/sendToKafka")
  try {
    const message = req.body;
    // Nos conectamos al producer de kafka
    await producer.connect();
    // Enviamos los eventos al consumer
    await producer.send({
      topic: 'kafka-topic',
      messages: [
        { value: JSON.stringify(message) },
      ],
    });
    console.log('Message sent to Kafka:', message);
    res.status(200).json('Message sent to Kafka');
  } catch (error) {
    console.error('Error sending message to Kafka:', error);
    res.status(500).json('Internal Server Error');
  }
});

// Endpoint de prueba
app.get('/', async (req, res) => {
  res.status(200).send('Server ok');
});

// Funcion que almaena los datos en mongoDb
async function saveToMongo(message) {
  await mongoClient.connect();

  // Set up MongoDB collection
  const db = mongoClient.db(dbName);
  const collection = db.collection('messages');

  try {
    console.log(message);
    await collection.insertOne(message);
  } catch (error) {
    console.error('Error saving to MongoDB:', error);
  }
}

// Inicializamos el servidor en el puerto 3000
app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});