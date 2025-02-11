const amqp = require('amqplib');

const QUEUE_NAME = 'envioML_from_callback'; // Cambia esto por el nombre de tu cola
const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672'; // Cambia esto por la URL de tu RabbitMQ

let reconnectAttempts = 0;
const maxReconnectAttempts = 10; // Número máximo de intentos de reconexión

const vaciarCola = async () => {
  let connection;
  let channel;

  const reconnect = async () => {
    try {
      if (reconnectAttempts >= maxReconnectAttempts) {
        console.error('Número máximo de intentos de reconexión alcanzado');
        return;
      }

      // Incrementar el contador de intentos de reconexión
      reconnectAttempts++;

      console.log(`Intentando reconectar... (Intento ${reconnectAttempts})`);

      // Intentar conectarse a RabbitMQ con un heartbeat de 60 segundos
      connection = await amqp.connect(RABBITMQ_URL + '?heartbeat=60');
      channel = await connection.createChannel();

      // Asegurarse de que la cola existe
      await channel.assertQueue(QUEUE_NAME, { durable: false });

      // Consumir mensajes de la cola
      channel.consume(QUEUE_NAME, (msg) => {
        if (msg !== null) {
          console.log(`Mensaje recibido: ${msg.content.toString()}`);
          // Aquí puedes procesar el mensaje si es necesario

          // Acknowledge el mensaje para eliminarlo de la cola
          channel.ack(msg);
        }
      }, { noAck: false });

      // Manejo de cierre y errores de conexión
      connection.on('error', (err) => {
        console.error('Error en la conexión de RabbitMQ:', err);
        if (err.code === 'ECONNRESET') {
          console.log('Se ha perdido la conexión, reintentando...');
          setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
        }
      });

      connection.on('close', () => {
        console.log('Conexión cerrada, reintentando...');
        setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
      });

    } catch (error) {
      console.error('Error al conectar a RabbitMQ:', error);
      setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
    }
  };

  await reconnect();
};

// Iniciar el proceso
vaciarCola();
