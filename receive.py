#!/usr/bin/env python
import pika, sys, os

def main():
    credentials = pika.PlainCredentials('seiwa', 'ADSads123')
    parameters = pika.ConnectionParameters('851e0741942a.sn.mynetname.net',5672,'/',credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='receive_data')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

    channel.basic_consume(queue='receive_data', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)