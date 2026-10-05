import boto3
import json

# Boto3 cogerá automáticamente las credenciales de la terminal
sqs = boto3.client('sqs', region_name='us-east-1')

queue_url = 'https://sqs.us-east-1.amazonaws.com/729017152804/v2x-events'

print("Esperando eventos V2X en la cola...")

response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    WaitTimeSeconds=5
)

if 'Messages' in response:
    for message in response['Messages']:
        body = json.loads(message['Body'])
        print(f"Evento recibido de {body['source_id']}: Tipo -> {body['event_type']}")
        
        if body['event_type'] == 'sudden braking':
            print(">>> ¡ALERTA DETECTADA! Frenada brusca en la vía.")
        
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        print("Mensaje procesado y eliminado de la cola SQS.")
else:
    print("No hay eventos nuevos en este momento.")