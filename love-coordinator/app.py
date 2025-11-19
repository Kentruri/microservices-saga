import pika
import time
import logging
from collections import defaultdict
import os

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [COORDINATOR] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

approval_state = defaultdict(lambda: {"mom": None, "dad": None})

def connect():
    while True:
        try:
             connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
             return connection
        except pika.exceptions.AMQPConnectionError:
            logging.warning("Esperando conexión con RabbitMQ...")
            time.sleep(5)

def decide(suitor_name):
    status = approval_state[suitor_name]

    if status["mom"] is None or status["dad"] is None:
        return None
        
    if status["mom"] == "rejected" or status["dad"] == "rejected":
        return "REJECTED"
        
    if status["mom"] == "approved" and status["dad"] == "approved":
        return "APPROVED"
        
    return None

def callback(ch, method, properties, body):
    msg = body.decode()
    
    try:
        _, parent, result, suitor_msg = msg.split(":", 3)
        suitor_name = suitor_msg.split()[0] 
        
    except ValueError:
        logging.error(f"Formato de mensaje no reconocido: {msg}")
        return
        
    approval_state[suitor_name][parent.lower()] = result.lower()

    final_status = decide(suitor_name)
    
    if final_status:
        final_msg = f"Relación con {suitor_name} fue {final_status}"
        
        channel.basic_publish(
            exchange='romantic-decision',
            routing_key='',
            body=final_msg.encode()
        )
        logging.info(f" Resultado FINAL enviado: {final_msg}")
        
        del approval_state[suitor_name]


connection = connect()
channel = connection.channel()

channel.exchange_declare(exchange='romantic-approval', exchange_type='fanout')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='romantic-approval', queue=queue_name)

channel.exchange_declare(exchange='romantic-decision', exchange_type='fanout')

logging.info("Esperando aprobaciones para tomar una decisión final... ")
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming() 