"""Vacía las tablas DynamoDB del sistema antes de cada experimento (N0 = 0).

Uso (desde la raíz del repo):  python3 experiments/limpiar_dynamo.py
Requiere boto3 y credenciales de AWS Academy configuradas en el portátil.
"""
import boto3

TABLAS = {"v2x-events-store": "event_id", "v2x-alerts-store": "alert_id"}

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

for nombre, clave in TABLAS.items():
    tabla = dynamodb.Table(nombre)
    borrados = 0
    kwargs = {"ProjectionExpression": "#k", "ExpressionAttributeNames": {"#k": clave}}
    while True:
        resp = tabla.scan(**kwargs)
        with tabla.batch_writer() as lote:
            for item in resp["Items"]:
                lote.delete_item(Key={clave: item[clave]})
                borrados += 1
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print(f"{nombre}: {borrados} elementos borrados")
