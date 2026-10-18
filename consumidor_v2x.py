import boto3
import json
import time

# Clientes de AWS
sqs = boto3.client('sqs', region_name='us-east-1')
logs = boto3.client('logs', region_name='us-east-1')

# Configuración
respuesta = sqs.get_queue_url(QueueName='v2x-events')
queue_url = respuesta['QueueUrl']
LOG_GROUP = '/v2x-mvp/alerts'
LOG_STREAM = 'consumidor-ec2'

# Intentamos crear el Log Stream en CloudWatch por si no existe
try:
    logs.create_log_group(logGroupName=LOG_GROUP)
except logs.exceptions.ResourceAlreadyExistsException:
    pass

try:
    logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
except logs.exceptions.ResourceAlreadyExistsException:
    pass

print("Iniciando V2X-SERVICE... Consola EC2: TODO | CloudWatch: SOLO ALERTAS")

while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )

    if 'Messages' in response:
        for message in response['Messages']:
            body = json.loads(message['Body'])
            
            # 1. Esto SIEMPRE sale en la pantalla del EC2
            print(f"\nEvento recibido de {body['source_id']}: Tipo -> {body['event_type']}")
            
            if body['event_type'] == 'sudden braking':
                alerta_msg = f">>> ¡ALERTA DETECTADA! Frenada brusca en la vía (Vehículo: {body['source_id']})."
                
                # 2. Esto sale en la pantalla del EC2
                print(alerta_msg)
                
                # 3. Esto envía SOLO la alerta a CloudWatch
                try:
                    logs.put_log_events(
                        logGroupName=LOG_GROUP,
                        logStreamName=LOG_STREAM,
                        logEvents=[{
                            'timestamp': int(time.time() * 1000),
                            'message': alerta_msg
                        }]
                    )
                except Exception as e:
                    print(f"Error enviando a CloudWatch: {e}")
            
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            print("Mensaje procesado y eliminado de la cola SQS.")
