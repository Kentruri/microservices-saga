import pika
import time
import sys
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [SUITOR] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
suitor_name = os.environ.get('SUITOR_NAME', sys.argv[1] if len(sys.argv) > 1 else "Arthur")
proposal_message = f"{suitor_name} desea formalizar con Ana."

def connect():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.warning("Esperando conexión con RabbitMQ...")
            time.sleep(5)

connection = connect()
channel = connection.channel()

EXCHANGE_NAME = 'romantic-proposal'
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout')

logging.info(f"💌 Enviando propuesta de {suitor_name}...")

channel.basic_publish(
    exchange=EXCHANGE_NAME,
    routing_key='',
    body=proposal_message.encode()
)

logging.info(f"✅ Propuesta enviada a '{EXCHANGE_NAME}': {proposal_message}")
connection.close()