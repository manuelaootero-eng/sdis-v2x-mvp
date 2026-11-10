import boto3
import json
import time
import threading
import requests
from flask import Flask, request, jsonify

# Clientes de AWS
sqs = boto3.client('sqs', region_name='us-east-1')
logs = boto3.client('logs', region_name='us-east-1')

# Configuración SQS y CloudWatch
respuesta = sqs.get_queue_url(QueueName='v2x-events')
queue_url = respuesta['QueueUrl']
LOG_GROUP = '/v2x-mvp/alerts'
LOG_STREAM = 'consumidor-ec2'

try:
    logs.create_log_group(logGroupName=LOG_GROUP)
except logs.exceptions.ResourceAlreadyExistsException:
    pass

try:
    logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
except logs.exceptions.ResourceAlreadyExistsException:
    pass

# Servidor HTTP para SNS (Tarea Manuela)
app = Flask(__name__)

@app.route('/sns-notifications', methods=['POST'])
def sns_notifications():
    try:
        data = json.loads(request.data)
    except ValueError:
        return jsonify({"status": "Invalid JSON"}), 400

    msg_type = request.headers.get('x-amz-sns-message-type')

    if msg_type == 'SubscriptionConfirmation':
        subscribe_url = data.get('SubscribeURL')
        if subscribe_url:
            requests.get(subscribe_url)
            print(f"✅ Suscripción a SNS confirmada automáticamente: {subscribe_url}")
        return "OK", 200

    elif msg_type == 'Notification':
        mensaje = data.get('Message')
        print(f"🚨 ALERTA GLOBAL RECIBIDA DE SNS: {mensaje}")
        return "OK", 200

    return "OK", 200

# Lógica original de lectura SQS en segundo plano
def leer_sqs_continuamente():
    print("Iniciando lectura de SQS en segundo plano...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    body = json.loads(message['Body'])
                    print(f"Evento recibido de {body.get('source_id', 'N/A')}: Tipo -> {body.get('event_type', 'N/A')}")
                    
                    if body.get('event_type') == 'sudden braking':
                        alerta_msg = f">>> ¡ALERTA DETECTADA! Frenada brusca en la vía (Vehículo: {body.get('source_id', 'N/A')})."
                        print(alerta_msg)
                        
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
        except Exception as e:
            print(f"Error leyendo SQS: {e}")
            time.sleep(2)

if __name__ == '__main__':
    print("Iniciando RSU (Servidor Web en puerto 80 + Procesamiento Edge)...")
    hilo_sqs = threading.Thread(target=leer_sqs_continuamente, daemon=True)
    hilo_sqs.start()
    app.run(host='0.0.0.0', port=80)