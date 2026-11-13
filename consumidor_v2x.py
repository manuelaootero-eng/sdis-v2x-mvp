import collections
import json
import os
import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import Flask, jsonify
from waitress import serve

from v2x_logic import (
    DeduplicadorEventos,
    DetectorHazard,
    MetricasV2X,
    clasificar_telemetria,
    validar_esquema_evento,
)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
SNS_TOPIC_NAME = os.getenv("SNS_TOPIC_NAME", "v2x-alerts")
DYNAMO_EVENTS_TABLE = os.getenv("DYNAMO_EVENTS_TABLE", "v2x-events-store")
DYNAMO_ALERTS_TABLE = os.getenv("DYNAMO_ALERTS_TABLE", "v2x-alerts-store")
LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP", "/v2x-mvp/alerts")
LOG_STREAM = os.getenv("CLOUDWATCH_LOG_STREAM", "v2x-service-cloud")
HTTP_PORT = int(os.getenv("SERVICE_HTTP_PORT", "8080"))

app = Flask(__name__)

sqs = None
logs = None
sns = None
dynamodb = None
queue_url = None
sns_topic_arn = None

ultimas_alertas = collections.deque(maxlen=50)
deduplicador = DeduplicadorEventos()
detector_hazard = DetectorHazard(time_window_s=5.0, max_distance_m=250.0)
metricas = MetricasV2X()


def inicializar_aws():
    global sqs, logs, sns, dynamodb, queue_url, sns_topic_arn
    boto_cfg = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2})
    sqs = boto3.client("sqs", region_name=AWS_REGION, config=boto_cfg)
    logs = boto3.client("logs", region_name=AWS_REGION, config=boto_cfg)
    sns = boto3.client("sns", region_name=AWS_REGION, config=boto_cfg)
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION, config=boto_cfg)

    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    sns_topic_arn = sns.create_topic(Name=SNS_TOPIC_NAME)["TopicArn"]

    for fn in (
        lambda: logs.create_log_group(logGroupName=LOG_GROUP),
        lambda: logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM),
    ):
        try:
            fn()
        except Exception:
            pass


def enviar_cloudwatch(mensaje: str):
    if logs is None:
        return
    try:
        logs.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": mensaje}],
        )
    except Exception as e:
        print(f" [CW WARN] {e}")


def publicar_sns(tipo_alerta: str, mensaje: str, metadata: dict):
    if sns is None or not sns_topic_arn:
        return
    try:
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"V2X Alert: {tipo_alerta}",
            Message=json.dumps({"alert_type": tipo_alerta, "message": mensaje, "metadata": metadata}),
        )
    except Exception as e:
        print(f" [SNS WARN] {e}")


def guardar_evento_dynamodb(nombre_tabla: str, item: dict, pk_field: str = "event_id") -> str:
    if dynamodb is None:
        return "STORED"
    tabla = dynamodb.Table(nombre_tabla)
    item_decimal = json.loads(json.dumps(item), parse_float=Decimal)
    try:
        tabla.put_item(Item=item_decimal, ConditionExpression=f"attribute_not_exists({pk_field})")
        return "STORED"
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return "DUPLICATE"
        raise


def procesar_evento(body: dict) -> bool:
    metricas.inc("received")

    valido, _ = validar_esquema_evento(body)
    if not valido:
        return True

    event_id = body["event_id"]
    if deduplicador.es_duplicado(event_id):
        metricas.inc("duplicates")
        return True

    vel = float(body["payload"]["velocidad"])
    acc = float(body["payload"]["aceleracion"])
    pos_x = float(body["location"]["posicion_x"])

    estado = clasificar_telemetria(vel, acc)
    body["derived_state"] = estado

    try:
        resultado_db = guardar_evento_dynamodb(DYNAMO_EVENTS_TABLE, body, pk_field="event_id")
    except Exception as e:
        deduplicador.vistos.pop(event_id, None)
        print(f" [DYNAMO ERROR] {e}")
        return False

    if resultado_db == "DUPLICATE":
        metricas.inc("duplicates")
        return True

    veh_id = body["source_id"]
    ts_str = body["event_time"]
    try:
        ts_unix = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        metricas.registrar_latencia((time.time() - ts_unix) * 1000.0)
    except ValueError:
        ts_unix = time.time()

    metricas.inc("accepted")
    metricas.inc("processed")
    metricas.inc("forwarded_or_persisted")

    if estado == "SUDDEN_BRAKING":
        msg_frenada = f"[SUDDEN_BRAKING] Frenada brusca en {veh_id} (X={pos_x}m, Accel={acc}m/s²)."
        print(f" 🚨 {msg_frenada}")
        alerta = {
            "alert_id": f"alt-{event_id}",
            "alert_type": "SUDDEN_BRAKING",
            "source_id": veh_id,
            "timestamp": ts_str,
            "posicion_x": pos_x,
            "message": msg_frenada,
        }
        ultimas_alertas.append(alerta)
        metricas.inc("alerts_triggered")
        enviar_cloudwatch(msg_frenada)
        publicar_sns("SUDDEN_BRAKING", msg_frenada, alerta)
        try:
            guardar_evento_dynamodb(DYNAMO_ALERTS_TABLE, alerta, pk_field="alert_id")
        except Exception as e:
            print(f" [DYNAMO WARN] {e}")

        implicados = detector_hazard.registrar_frenada(veh_id, ts_unix, pos_x)
        if implicados:
            msg_hazard = f"[HAZARD_DETECTED] Peligro cerca de X={pos_x}m: frenadas simultáneas ({', '.join(implicados)})"
            print(f" ⚠️ {msg_hazard}")
            hazard = {
                "alert_id": f"haz-{uuid.uuid4().hex[:8]}",
                "alert_type": "HAZARD_DETECTED",
                "vehicles": implicados,
                "timestamp": ts_str,
                "posicion_x": pos_x,
                "message": msg_hazard,
            }
            ultimas_alertas.append(hazard)
            metricas.inc("alerts_triggered")
            enviar_cloudwatch(msg_hazard)
            publicar_sns("HAZARD_DETECTED", msg_hazard, hazard)
            try:
                guardar_evento_dynamodb(DYNAMO_ALERTS_TABLE, hazard, pk_field="alert_id")
            except Exception as e:
                print(f" [DYNAMO WARN] {e}")

    return True


def leer_sqs_continuamente():
    while True:
        try:
            resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5)
            mensajes = resp.get("Messages", [])
            if not mensajes:
                continue

            borrados = []
            for msg in mensajes:
                try:
                    body = json.loads(msg["Body"])
                    if procesar_evento(body):
                        borrados.append({"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]})
                except ValueError:
                    borrados.append({"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]})

            if borrados:
                sqs.delete_message_batch(QueueUrl=queue_url, Entries=borrados)
        except Exception as e:
            print(f" [SQS ERROR] {e}")
            time.sleep(2.0)


@app.route("/alerts", methods=["GET"])
def get_alerts():
    return jsonify(list(ultimas_alertas)), 200


@app.route("/health", methods=["GET"])
def get_health():
    return jsonify({"service": "V2X-SERVICE", "status": "OK", "active_alerts": len(ultimas_alertas)}), 200


@app.route("/metrics", methods=["GET"])
def get_metrics():
    return jsonify(metricas.resumen(extra={"service": "V2X-SERVICE"})), 200


@app.route("/metrics/reset", methods=["POST"])
def reset_metrics():
    metricas.reset()
    return jsonify({"service": "V2X-SERVICE", "status": "METRICS_RESET"}), 200


def main():
    inicializar_aws()
    threading.Thread(target=leer_sqs_continuamente, daemon=True).start()
    print(f"Iniciando V2X-SERVICE en puerto {HTTP_PORT}...")
    serve(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()