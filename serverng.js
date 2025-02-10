const amqp = require('amqplib');
const redis = require('redis');
const axios = require('axios'); // Para manejar solicitudes HTTP

/*-----------------------------------------------------*/
class EnvioProcessor {
  constructor() {
    this.token = null;
    this.dataEnvioML = null;
    this.dataRedisEnvio = null;
    this.dataRedisFecha = null;
	this.sellerid = null;
	this.shipmentid = null;
	this.clave = null;
  }
  
  setClave(claveabuscar){
	  this.clave = claveabuscar;
  }
  
  setSellerid(sellerid){
	  this.sellerid = sellerid;
  }
  
  setShipmentid(shipmentid){
	  this.shipmentid = shipmentid;
  }

  // Setter para token
  setToken(token) {
    this.token = token;
  }

  // Setter para dataEnvioML
  setDataEnvioML(dataEnvioML) {
    this.dataEnvioML = dataEnvioML;
  }

  // Setter para dataRedisEnvio
  setDataRedisEnvio(dataRedisEnvio) {
    this.dataRedisEnvio = dataRedisEnvio;
  }

  // Setter para dataRedisFecha
  setDataRedisFecha(dataRedisFecha) {
	  if(dataRedisFecha != ""){
		 this.dataRedisFecha = JSON.parse(dataRedisFecha);
	  }
  }
  
  //actualizo fecha redis
  async actualizaFechasRedis(ml_clave, ml_fechas, ml_estado, ml_subestado){

	  let data = this.dataRedisFecha;
	  data.fecha = ml_fechas;
	  data.clave = ml_estado+"-"+ml_subestado;
	  data.estado = ml_estado;
	  data.subestado = ml_subestado;
	  
	  //console.log("actualizaFechasRedis", data);
	  
	  if (!redisClient.isOpen) await redisClient.connect();
	  await redisClient.hDel("estadoFechasML", this.clave);
	  await redisClient.hSet("estadoFechasML", this.clave, JSON.stringify(data));	
  }
  
  //elimino claves si es entregado o cancelado
  async eliminarCLavesEntregadosYCancelados(){
	//  console.log("PROCSO DE VACIAR REDIS PARA ENTREGADOS");
	  if (!redisClient.isOpen) await redisClient.connect();
	  await redisClient.hDel("estadoFechasML", this.clave);
	  await redisClient.hDel("estadosEnviosML", this.clave);
  }
  
  //proceso la logica para obtener estados y fecha
  async obtenerFechaFinalyestado() {
	const ml_fechas = this.dataEnvioML.status_history;
	const ml_estado = this.dataEnvioML.status;
	const ml_subestado = this.dataEnvioML.substatus;
  
	// Lógica para determinar el estado y la fecha
	let estadonumero = -1;
	let fecha = null;
  
	if (ml_estado === "delivered") {
	  estadonumero = 5;
	  fecha = ml_fechas.date_delivered;
	} else if (ml_estado === "cancelled") {
	  estadonumero = 8;
	  fecha = ml_fechas.date_cancelled;
	} else if (ml_estado === "shipped") {
	  estadonumero = 2;
	  fecha = ml_fechas.date_shipped;
  
	  if (ml_subestado === "delivery_failed" || ml_subestado === "receiver_absent") {
		estadonumero = 6;
		fecha = ml_fechas.date_first_visit;
	  } else if (ml_subestado === "claimed_me") {
		estadonumero = 8;
		fecha = ml_fechas.date_first_visit;
	  } else if (ml_subestado === "buyer_rescheduled") {
		estadonumero = 12;
		fecha = ml_fechas.date_first_visit;
	  }
	} else if (ml_estado === "not_delivered") {
	  estadonumero = 8;
	  fecha = ml_fechas.date_returned;
	}
  
	return { estado: estadonumero, fecha: fecha };
  }
  //envio los datos a server estado
  async sendToServerEstado(dateE){
	  	  
	  try {
		// Establecer conexión con RabbitMQ
		const connection = await amqp.connect({
		  protocol: 'amqp',
		  hostname: '158.69.131.226',
		  port: 5672,
		  username: 'lightdata',
		  password: 'QQyfVBKRbw6fBb',
		  heartbeat: 30, // Heartbeat cada 30 segundos
		});

		// Crear un canal
		const channel = await connection.createChannel();

		// Asegurar que la cola/clasve existe
		await channel.assertQueue("srvshipmltosrvstates", {
		  durable: true, // La cola persiste incluso si RabbitMQ se reinicia
		});

		// Convertir el dato a JSON (en caso de ser un objeto)
		const message = typeof dateE === 'string' ? dateE : JSON.stringify(dateE);

		// Enviar el mensaje a la cola
		channel.sendToQueue("srvshipmltosrvstates", Buffer.from(message), {
		  persistent: true, // Asegurar que el mensaje persista
		});

		//console.log(`Mensaje enviado al canal/cola srvshipmltosrvstates: ${message}`);

		// Cerrar conexión después de enviar el mensaje
		setTimeout(() => {
		  channel.close();
		  connection.close();
		}, 500); // Esperar un poco para asegurarse de que se envíe
	  } catch (error) {
		console.error('Error al enviar el mensaje:', error);
	  }
	  
  }
  
  //actualizo ficha envio
  async actualizoDataRedis(){
	//  console.log("actualizoDataRedis fecha sincronizacion");
	  const fechaact = await getCurrentDateInArgentina();
	  this.dataRedisEnvio.fechaActualizacion = fechaact;
	  if (!redisClient.isOpen) await redisClient.connect();
	  await redisClient.hDel("estadosEnviosML", this.clave);
	  await redisClient.hSet("estadosEnviosML", this.clave, JSON.stringify(this.dataRedisEnvio));	
  }

  // Método procesar
  async procesar() {
	//console.log("procesar");
  
	// Verificar si los datos necesarios están presentes
	if (!this.token || !this.dataEnvioML || !this.dataRedisEnvio) {
	  console.error("Faltan datos para procesar el envío.");
	  return { status: "error", message: "Faltan datos para procesar el envío." };
	}
  
	// Si dataRedisFecha no está definido, crearlo con valores por defecto
	if (!this.dataRedisFecha) {
	  console.error("Faltan datos para procesar el envío (dataRedisFecha).");
  
	  const ml_fechas = this.dataEnvioML.status_history;
	  const ml_estado = this.dataEnvioML.status;
	  const ml_subestado = this.dataEnvioML.substatus;
	  const ml_clave = `${ml_estado}-${ml_subestado}`;
  
	  // Crear dataRedisFecha con valores por defecto
	  this.dataRedisFecha = {
		fecha: ml_fechas,
		clave: ml_clave,
		estado: ml_estado,
		subestado: ml_subestado,
	  };
  
	  // Actualizar Redis con los nuevos datos
	  await this.actualizaFechasRedis(ml_clave, ml_fechas, ml_estado, ml_subestado);
	}
  
	// Actualizar la fecha de sincronización en Redis
	await this.actualizoDataRedis();
  
	// Obtener el estado y la fecha final
	const response = await this.obtenerFechaFinalyestado();
	const estadonumero = response.estado;
	let fecha = response.fecha;
  
	// Convertir la fecha a la zona horaria de Argentina
	fecha = await convertToArgentinaTime(fecha);
  
	// Enviar los datos al servidor de estados
	const dataE = {
	  didempresa: this.dataRedisEnvio.didEmpresa,
	  didenvio: this.dataRedisEnvio.didEnvio,
	  estado: estadonumero,
	  subestado: this.dataEnvioML.substatus,
	  estadoML: this.dataEnvioML.status,
	  fecha: fecha,
	  quien: 0,
	};
  
	await this.sendToServerEstado(dataE);
  
	// Si el estado es "entregado" o "cancelado", eliminar las claves de Redis
	if (estadonumero === 5 || estadonumero === 8) {
	  await this.eliminarCLavesEntregadosYCancelados();
	}
  
	// Retornar el resultado del procesamiento
	const result = {
	  status: "success",
	  message: "Envío procesado correctamente",
	};
  
	//console.log("Resultado del procesamiento:", result);
	return result;
  }
}
/*-----------------------------------------------------*/

let Atokens = [];
const claveEstadoRedis = 'estadosEnviosML';
const claveEstadoFechasML = 'estadoFechasML';


const redisClient = redis.createClient({
  socket: {
    host: '192.99.190.137',
    port: 50301,
  },
  password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
  console.error('Error al conectar con Redis:', err);
});

// Función principal
async function main() {
  try {
    await redisClient.connect();
  //  console.log('Conexión exitosa a Redis.');

    // Obtener los datos iniciales de Redis
    await getTokenRedis();
	

    // Consumir mensajes de RabbitMQ
	await consumirMensajes();
	
	//await simular();
	
  } catch (error) {
    console.error('Error en la ejecución principal:', error);
  } finally {
    await redisClient.disconnect(); // Asegúrate de cerrar la conexión al salir
  }
}

//funcion para obtener la fecha actual
async function getCurrentDateInArgentina() {
  // Obtener la fecha actual en UTC
  const now = new Date();

  // Calcular el offset de Argentina (-3 horas en milisegundos)
  const argentinaOffset = -3 * 60 * 60 * 1000;

  // Ajustar la fecha actual a la hora de Argentina
  const argentinaTime = new Date(now.getTime() + argentinaOffset);

  // Formatear la fecha en "yyyy-MM-dd HH:mm:ss"
  const year = argentinaTime.getFullYear();
  const month = String(argentinaTime.getMonth() + 1).padStart(2, '0');
  const day = String(argentinaTime.getDate()).padStart(2, '0');
  const hours = String(argentinaTime.getHours()).padStart(2, '0');
  const minutes = String(argentinaTime.getMinutes()).padStart(2, '0');
  const seconds = String(argentinaTime.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

//funcion para transforamr fecha
async function convertToArgentinaTime(dateString) {
  // Crear el objeto Date desde la cadena ISO
  const date = new Date(dateString);

  // Obtener el tiempo UTC de la fecha
  const utcTime = date.getTime();

  // Calcular el offset de Argentina en milisegundos (-3 horas)
  const argentinaOffset = -3 * 60 * 60 * 1000;

  // Crear una nueva fecha ajustada a Argentina
  const argentinaTime = new Date(utcTime + argentinaOffset);

  // Formatear la fecha como "yyyy-MM-dd HH:mm:ss"
  const formattedDate = argentinaTime.toISOString().slice(0, 19).replace('T', ' ');
  return formattedDate;
}

//funcion para obtener los datos de envios de ML
async function obtenerDatosEnvioML(shipmentid, token) {
	try {
	  const url = `https://api.mercadolibre.com/shipments/${shipmentid}`;
	  const response = await axios.get(url, {
		headers: {
		  Authorization: `Bearer ${token}`,
		},
	  });
  
	  // Verificar si la respuesta es válida
	  if (response.data && response.data.id) {
		return response.data; // Devuelve los datos del envío
	  } else {
		console.error(`No se encontraron datos válidos para el envío ${shipmentid}.`);
		return null;
	  }
	} catch (error) {
	  console.error(`Error al obtener datos del envío ${shipmentid} desde Mercado Libre:`, error.message);
	  return null; // Devuelve null en caso de error
	}
  }

// Obtener tokens desde Redis
async function getTokenRedis() {
  try {
    const data = await redisClient.get('token');
    Atokens = JSON.parse(data || '{}');
   // console.log('Datos de Redis:', Atokens);
  } catch (error) {
   // console.error('Error al obtener tokens de Redis:', error);
  }
}

async function getTokenForSeller(seller_id) {
    try {
        // Usa HGET para obtener el token del hash "token" para el seller_id especificado
		if (!redisClient.isOpen) await redisClient.connect();
		
        const token = await redisClient.hGet('token', seller_id);

        if (token) {
         //   console.log(`Token encontrado para seller_id ${seller_id}: ${token}`);
            return token;
        } else {
      //      console.log(`No se encontró token para seller_id ${seller_id}`);
            return null;
        }
    } catch (error) {
      //  console.error('Error al obtener el token de Redis:', error);
        return null;
    }
}

// Extraer ID de envío desde la clave
function extractKey(resource) {
  const match = resource.match(/\/shipments\/(\d+)/);
  return match ? match[1] : null;
}

// Consumir mensajes de RabbitMQ
async function consumirMensajes() {
	let connection;
	let channel;
  
	const reconnect = async () => {
	  try {
		if (connection) await connection.close();
		if (channel) await channel.close();
  
		connection = await amqp.connect({
		  protocol: 'amqp',
		  hostname: '158.69.131.226',
		  port: 5672,
		  username: 'lightdata',
		  password: 'QQyfVBKRbw6fBb',
		  heartbeat: 30,
		});
  
		channel = await connection.createChannel();
		await channel.assertQueue('estadoEnvioML_from_callback', { durable: false });
  
		//console.log('Conexión a RabbitMQ establecida.');

		await channel.prefetch(3000); 


  
		channel.consume('estadoEnvioML_from_callback', async (mensaje) => {
		  if (mensaje) {
			const data = JSON.parse(mensaje.content.toString());
			const shipmentid = extractKey(data['resource']);
			const sellerid = data['sellerid'];
			const claveabuscar = `${sellerid}-${shipmentid}`;
  
			if (!redisClient.isOpen) await redisClient.connect();
			const exists = await redisClient.hExists(claveEstadoRedis, claveabuscar);
  
			//console.log('Nuevo envío recibido:', data);
  
			if (exists) {
			  let envioData = await redisClient.hGet(claveEstadoRedis, claveabuscar);
			  envioData = JSON.parse(envioData);
			  const token = await getTokenForSeller(sellerid);
  
			  if (!token) {
			//	console.error(`No se encontró token para seller_id ${sellerid}.`);
				return;
			  }
  
			  const envioML = await obtenerDatosEnvioML(shipmentid, token);
  
			  if (!envioML) {
			//	console.error(`No se pudieron obtener datos para el envío ${shipmentid}.`);
				return;
			  }
  
			  let envioRedisFecha = await redisClient.hGet(claveEstadoFechasML, claveabuscar);
			  if (!envioRedisFecha) {
			//	console.log(`No se encontró dataRedisFecha para la clave ${claveabuscar}. Creando uno nuevo...`);
				envioRedisFecha = JSON.stringify({
				  fecha: envioML.status_history,
				  clave: `${envioML.status}-${envioML.substatus}`,
				  estado: envioML.status,
				  subestado: envioML.substatus,
				});
			  }
  
			  const envio = new EnvioProcessor();
			  envio.setToken(token);
			  envio.setSellerid(sellerid);
			  envio.setShipmentid(shipmentid);
			  envio.setClave(claveabuscar);
			  envio.setDataEnvioML(envioML);
			  envio.setDataRedisEnvio(envioData);
			  envio.setDataRedisFecha(envioRedisFecha);
			  const resultado = await envio.procesar();
			  //console.log("Resultado final:", resultado);
			} else {
			  //console.log(`Clave ${claveabuscar} no encontrada en Adata.`);
			}
			channel.ack(mensaje);
		  }
		}, { noAck: false });
  
		// Manejo de errores en el canal
		channel.on('error', (err) => {
		  console.error('Error en el canal:', err);
		  setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
		});
  
		// Manejo de cierre del canal
		channel.on('close', () => {
		  console.error('Canal cerrado. Intentando reconectar...');
		  setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
		});
  
		// Manejo de errores en la conexión
		connection.on('error', (err) => {
		  console.error('Error en la conexión:', err);
		  setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
		});
  
		// Manejo de cierre de conexión
		connection.on('close', () => {
		  console.error('Conexión cerrada. Intentando reconectar...');
		  setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
		});
	  } catch (err) {
		console.error('Error al conectar a RabbitMQ:', err);
		setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
	  }
	};
  
	await reconnect();
  }
/*--------------------------------------------------------*/
async function simular(){
	
	//179907718-44416729582
	let data = {"resource": "/shipments/44416729582" , "sellerid":"179907718"};
	const shipmentid = extractKey(data['resource']);
	const sellerid = data['sellerid'];
	const claveabuscar = `${sellerid}-${shipmentid}`;
	if (!redisClient.isOpen) await redisClient.connect();
	let exists = await redisClient.hExists(claveEstadoRedis, claveabuscar);
	console.log('Nuevo envío recibido:', data);

	if (exists) {
		
		let envioData = await redisClient.hGet(claveEstadoRedis, claveabuscar);
		envioData = JSON.parse(envioData);
		const token = await getTokenForSeller(sellerid);
		const envioML = await obtenerDatosEnvioML(shipmentid, token);
		let envioRedisFecha = "";
		
		let exists = await redisClient.hExists(claveEstadoFechasML, claveabuscar);
		if(exists){
			envioRedisFecha = await redisClient.hGet(claveEstadoFechasML, claveabuscar);
		}
		
		console.log("envioRedisFecha", envioRedisFecha);
		//process.exit(0);
		
		if(token){
			const envio = new EnvioProcessor();
			envio.setToken(token);
			envio.setSellerid(sellerid);
			envio.setShipmentid(shipmentid);
			envio.setClave(claveabuscar);
			envio.setDataEnvioML(envioML);
			envio.setDataRedisEnvio(envioData);
			envio.setDataRedisFecha(envioRedisFecha);	
			const resultado = await  envio.procesar();
			console.log("Resultado final:", resultado);
			process.exit(0);
		}

		if (token) {
		  console.log('Procesando con el token:', token);

		   // Aquí puedes continuar con la lógica de Mercado Libre y notificación
		   
		   await updateFechaEstadoML(sellerid, shipmentid, envioML, envioData);
			
		   
			process.exit(0); 
		} else {
		  console.log(`No se encontró token para seller_id ${sellerid}`);
		}
		
	} else {
		console.log(`Clave ${claveabuscar} no encontrada en Adata.`);
	}
	
}

// Llamar a la función principal
main();

