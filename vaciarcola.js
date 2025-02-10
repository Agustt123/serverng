const amqp = require('amqplib');

const QUEUE_NAME = 'envioML_from_callback'; // Cambia esto por el nombre de tu cola
const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672'; // Cambia esto por la URL de tu RabbitMQ

const vaciarCola = async () => {
  try {
    // Conectar a RabbitMQ
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    // Asegurarse de que la cola existe
    await channel.assertQueue(QUEUE_NAME, { durable: false });

    //console.log(`Escuchando mensajes en la cola: ${QUEUE_NAME}`);

    // Consumir mensajes de la cola
    channel.consume(QUEUE_NAME, (msg) => {
      if (msg !== null) {
      //  console.log(`Mensaje recibido: ${msg.content.toString()}`);
        // Aquí puedes procesar el mensaje si es necesario

        // Acknowledge el mensaje para eliminarlo de la cola
        channel.ack(msg);
      }
    }, { noAck: false });

  } catch (error) {
    //console.error('Error al conectar a RabbitMQ:', error);
  }
};

// Iniciar el proceso
vaciarCola();
