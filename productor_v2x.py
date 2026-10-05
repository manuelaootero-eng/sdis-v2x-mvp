import boto3
import json
import random
import time

# Boto3 lee automáticamente las credenciales de las variables de entorno de la terminal
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/729017152804/v2x-events'

# Lista de al menos 5 vehículos exigidos por el requisito
vehiculos = ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"]
tipos_evento = ["position", "sudden braking", "hazard"]

print("Iniciando simulación de tráfico para 5 vehículos...")

for i in range(5):
    source_id = random.choice(vehiculos)
    event_type = random.choice(tipos_evento)
    
    # Si el tipo es frenada brusca, simulamos una velocidad baja o intensidad alta para la alerta
    speed = random.randint(10, 30) if event_type == "sudden braking" else random.randint(50, 120)
    
    evento_v2x = {
        "event_id": f"evt-{random.randint(1000, 9999)}",
        "source_id": source_id,
        "event_type": event_type,
        "payload": {
            "speed": speed,
            "braking_intensity": "high" if event_type == "sudden braking" else "normal"
        }
    }

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(evento_v2x)
    )

    print(f"Enviado -> Vehículo: {source_id} | Tipo: {event_type} | ID SQS: {response['MessageId']}")
    time.sleep(0.5)

print("Simulación de lote completada con éxito.")
