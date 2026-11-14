import collections
import json
import os
import threading
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
import requests
from waitress import serve

from v2x_logic import (
    ControladorCooldown,
    DeduplicadorEventos,
    MetricasV2X,
    clasificar_telemetria,
    validar_esquema_evento,
)

RSU_ID = os.getenv("RSU_ID", "rsu-1")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "v2x-events")
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
HTTP_PORT = int(os.getenv("RSU_HTTP_PORT", "80"))
MAX_BUFFER_SIZE = int(os.getenv("RSU_MAX_BUFFER", "1000"))
EDGE_SAMPLE_RATE = int(os.getenv("EDGE_SAMPLE_RATE", "3"))

app = Flask(__name__)

sqs_buffer = collections.deque()
buffer_lock = threading.Lock()
en_fallo_sqs = False

deduplicador_mqtt = DeduplicadorEventos()
deduplicador_sns = DeduplicadorEventos()
cooldown_alerta_local = ControladorCooldown(cooldown_seconds=8.0)
metricas = MetricasV2X()
contador_rutinario_veh = {}

mqtt_client = None
sqs_client = None
queue_url = None


def publicar_alerta_mqtt_local(tipo_alerta: str, mensaje: str, origen: str = "RSU_EDGE"):
    metricas.inc("alerts_triggered")
    if mqtt_client is None:
        return
    payload = {
        "rsu_id": RSU_ID,
        "alert_type": tipo_alerta,
        "origin": origen,
        "timestamp": time.time(),
        "message": mensaje,
    }
    mqtt_client.publish(f"v2x/{RSU_ID}/alerts", json.dumps(payload), qos=1)


def encolar_para_sqs(evento: dict):
    """Política drop-oldest: si el buffer está lleno se descarta el evento más antiguo."""
    with buffer_lock:
        if len(sqs_buffer) >= MAX_BUFFER_SIZE:
            sqs_buffer.popleft()
            metricas.inc("dropped_buffer")
        sqs_buffer.append(evento)


def _reencolar_frente(eventos: list):
    """
    Reinserta eventos al principio del buffer conservando su orden original.
    Son los más antiguos: si no caben, se descartan ellos (coherente con drop-oldest).
    """
    with buffer_lock:
        for ev in reversed(eventos):
            if len(sqs_buffer) >= MAX_BUFFER_SIZE:
                metricas.inc("dropped_buffer")
                continue
            sqs_buffer.appendleft(ev)


def enviar_lote_sqs_una_vez() -> bool:
    """Extrae hasta 10 eventos del buffer y los envía a SQS. True si al menos uno se entregó."""
    global queue_url, en_fallo_sqs
    with buffer_lock:
        lote = [sqs_buffer.popleft() for _ in range(min(10, len(sqs_buffer)))]

    if not lote:
        return False

    try:
        if queue_url is None and sqs_client is not None:
            queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]

        entries = [{"Id": f"msg-{i}", "MessageBody": json.dumps(ev)} for i, ev in enumerate(lote)]
        resp = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)

        exitosos = len(resp.get("Successful", []))
        fallidos = resp.get("Failed", [])

        if fallidos:
            ids_fallidos = {f["Id"] for f in fallidos}
            eventos_fallidos = [ev for i, ev in enumerate(lote) if f"msg-{i}" in ids_fallidos]
            _reencolar_frente(eventos_fallidos)

        if exitosos > 0:
            metricas.inc("forwarded_or_persisted", exitosos)
            if en_fallo_sqs:
                ts_now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                print(f" ✅ [RSU RECOVERY] {ts_now} - Conexión SQS restaurada. Drenando buffer ({len(sqs_buffer)} restantes).")
                en_fallo_sqs = False
        return exitosos > 0
    except Exception as e:
        if not en_fallo_sqs:
            ts_now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            print(f" ⚠️ [RSU OUTAGE] {ts_now} - Fallo enviando a SQS ({type(e).__name__}). Reteniendo en buffer local.")
            en_fallo_sqs = True
        _reencolar_frente(lote)
        return False


def worker_envio_sqs():
    while True:
        if not enviar_lote_sqs_una_vez():
            time.sleep(0.5)


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        topic = f"v2x/{RSU_ID}/telemetry"
        client.subscribe(topic, qos=1)
        print(f" [RSU {RSU_ID}] Suscrito a '{topic}'")
    else:
        print(f" [RSU {RSU_ID}] Conexión MQTT rechazada rc={rc}")


def on_mqtt_message(client, userdata, msg):
    try:
        metricas.inc("received")
        try:
            evento = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return

        valido, _ = validar_esquema_evento(evento)
        if not valido:
            return

        if deduplicador_mqtt.es_duplicado(evento["event_id"]):
            metricas.inc("duplicates")
            return

        metricas.inc("accepted")

        try:
            ts_ev = datetime.fromisoformat(evento["event_time"].replace("Z", "+00:00")).timestamp()
            metricas.registrar_latencia((time.time() - ts_ev) * 1000.0)
        except Exception:
            pass

        vel = float(evento["payload"]["velocidad"])
        acc = float(evento["payload"]["aceleracion"])
        veh_id = evento["source_id"]
        clasificacion = clasificar_telemetria(vel, acc)

        evento["rsu_id"] = RSU_ID
        evento["edge_classification"] = clasificacion
        metricas.inc("processed")

        # 1. Alerta inmediata local y prioridad hacia SQS si hay frenada brusca (<= -4.5 m/s²)
        if clasificacion == "SUDDEN_BRAKING":
            if cooldown_alerta_local.puede_disparar(veh_id):
                msg_local = f"Frenada brusca local en {veh_id} (X={evento['location']['posicion_x']}m)"
                print(f" 🚨 [RSU LOCAL ALERT] {msg_local}")
                publicar_alerta_mqtt_local("LOCAL_SUDDEN_BRAKING", msg_local)
            encolar_para_sqs(evento)
            return

        # 2. Filtrado Edge: muestreo 1 de cada EDGE_SAMPLE_RATE mensajes rutinarios por vehículo
        cuenta = contador_rutinario_veh.get(veh_id, 0) + 1
        contador_rutinario_veh[veh_id] = cuenta
        if cuenta % EDGE_SAMPLE_RATE == 0:
            encolar_para_sqs(evento)
        else:
            metricas.inc("filtered_edge")
    except Exception as e:
        print(f" [RSU ERROR] Excepción en on_mqtt_message: {e}")


@app.route("/sns-notifications", methods=["POST"])
def sns_notifications():
    data = request.get_json(force=True, silent=True) or {}
    msg_type = data.get("Type") or request.headers.get("x-amz-sns-message-type")
    msg_id = data.get("MessageId")

    if msg_type == "SubscriptionConfirmation" and data.get("SubscribeURL"):
        try:
            requests.get(data["SubscribeURL"], timeout=5.0)
            print(f" ✅ [RSU {RSU_ID}] Suscripción SNS confirmada.")
        except requests.RequestException as e:
            print(f" [RSU ERROR] Fallo confirmando SNS: {e}")
            return jsonify({"status": "Confirmation failed"}), 500
    elif msg_type == "Notification":
        if msg_id and deduplicador_sns.es_duplicado(msg_id):
            metricas.inc("duplicates")
            return "OK", 200
        mensaje = data.get("Message", "")
        print(f" 📡 [RSU {RSU_ID}] Alerta SNS recibida -> Difundiendo por MQTT: {mensaje}")
        publicar_alerta_mqtt_local("GLOBAL_SNS_ALERT", mensaje, origen="V2X_SERVICE_SNS")

    return "OK", 200


@app.route("/health", methods=["GET"])
def health():
    with buffer_lock:
        buf_len = len(sqs_buffer)
    return jsonify({"rsu_id": RSU_ID, "status": "OK", "buffered_events": buf_len}), 200


@app.route("/metrics", methods=["GET"])
def get_metrics():
    with buffer_lock:
        buf_len = len(sqs_buffer)
    return jsonify(metricas.resumen(extra={"rsu_id": RSU_ID, "buffer_depth": buf_len})), 200


@app.route("/metrics/reset", methods=["POST"])
def reset_metrics():
    metricas.reset()
    return jsonify({"rsu_id": RSU_ID, "status": "METRICS_RESET"}), 200


def main():
    global sqs_client, mqtt_client
    boto_cfg = Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 1})
    sqs_client = boto3.client("sqs", region_name=AWS_REGION, config=boto_cfg)

    mqtt_client = mqtt.Client(client_id=f"edge-{RSU_ID}")
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=5)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.connect_async(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
    mqtt_client.loop_start()

    threading.Thread(target=worker_envio_sqs, daemon=True).start()
    print(f"Iniciando {RSU_ID} (MQTT {MQTT_BROKER_PORT} | HTTP {HTTP_PORT})...")
    serve(app, host="0.0.0.0", port=HTTP_PORT)


if __name__ == "__main__":
    main()
