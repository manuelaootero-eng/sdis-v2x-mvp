import argparse
import json
import random
import threading
import time
import uuid
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

from v2x_logic import BRAKING_THRESHOLD, HIGHWAY_LENGTH_M

print_lock = threading.Lock()
stop_event = threading.Event()

STAT_KEYS = ["generated", "sent_unique", "mqtt_publishes", "dropped", "delayed", "duplicated_extra", "publish_failed"]


def _cerrar_cliente(client):
    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass


def _lanzar_timer(segundos: float, fn, args=()):
    t = threading.Timer(segundos, fn, args=args)
    t.daemon = True
    t.start()
    return t


class VehiculoOBU(threading.Thread):
    def __init__(self, veh_id: str, pos_inicial: float, vel_ms: float, dt: float, rsu_endpoints: dict, fault_cfg: dict):
        super().__init__(daemon=True)
        self.veh_id = veh_id
        self.posicion_x = float(pos_inicial)
        self.vel_base_ms = float(vel_ms)
        self.velocidad_ms = float(vel_ms)
        self.aceleracion = 0.0
        self.dt = float(dt)
        self.rsu_endpoints = rsu_endpoints
        self.fault_cfg = fault_cfg
        self.tick = 0

        self.stats = {k: 0 for k in STAT_KEYS}
        self.stats_lock = threading.Lock()

        self.rsu_actual = "rsu-1" if self.posicion_x < 1000.0 else "rsu-2"
        self.mqtt_client = None
        self._conectar_mqtt(self.rsu_actual)

    def _inc(self, clave: str, n: int = 1):
        with self.stats_lock:
            self.stats[clave] += n

    def _conectar_mqtt(self, rsu_id: str):
        anterior = self.mqtt_client
        host, port = self.rsu_endpoints[rsu_id]
        client = mqtt.Client(client_id=f"{self.veh_id}-{rsu_id}-{uuid.uuid4().hex[:6]}")
        client.reconnect_delay_set(min_delay=1, max_delay=5)

        def on_connect(c, userdata, flags, rc):
            if rc == 0:
                c.subscribe(f"v2x/{rsu_id}/alerts", qos=1)

        def on_message(c, userdata, msg):
            try:
                alerta = json.loads(msg.payload.decode("utf-8"))
                with print_lock:
                    print(f" 📲 [{self.veh_id}] ALERTA RECIBIDA en {rsu_id.upper()}: {alerta.get('message')}")
            except Exception as e:
                with print_lock:
                    print(f" [WARN] Error procesando alerta MQTT en {self.veh_id}: {e}")

        client.on_connect = on_connect
        client.on_message = on_message
        try:
            client.connect_async(host, port, keepalive=60)
            client.loop_start()
        except Exception as e:
            with print_lock:
                print(f" [MQTT WARN] {self.veh_id} fallo conectando a {rsu_id} ({host}:{port}): {e}")
        self.mqtt_client = client

        if anterior is not None:
            # Cierre diferido: los mensajes retardados dirigidos a la RSU anterior aún deben poder salir por su cliente
            _lanzar_timer(self.fault_cfg["max_delay"] + 1.0, _cerrar_cliente, args=(anterior,))

    def cerrar(self):
        if self.mqtt_client is not None:
            _cerrar_cliente(self.mqtt_client)
            self.mqtt_client = None

    def _publicar(self, topic: str, payload_str: str, es_unico: bool = True, client=None) -> bool:
        cli = client if client is not None else self.mqtt_client
        if cli is not None:
            try:
                info = cli.publish(topic, payload_str, qos=1)
                if info.rc == mqtt.MQTT_ERR_SUCCESS:
                    self._inc("mqtt_publishes")
                    if es_unico:
                        self._inc("sent_unique")
                    return True
            except Exception:
                pass
        if es_unico:
            self._inc("publish_failed")
        return False

    def _enviar_con_fallos(self, topic: str, payload_str: str) -> str:
        self._inc("generated")

        if random.random() < self.fault_cfg["drop_prob"]:
            self._inc("dropped")
            return "DROPPED"

        if random.random() < self.fault_cfg["delay_prob"]:
            self._inc("delayed")
            delay_s = random.uniform(0.2, max(0.2, self.fault_cfg["max_delay"]))
            # Se captura el cliente actual: el mensaje sale hacia la RSU a la que iba dirigido aunque haya handover
            _lanzar_timer(delay_s, self._publicar, args=(topic, payload_str, True, self.mqtt_client))
            return f"DELAYED({delay_s:.1f}s)"

        ok = self._publicar(topic, payload_str, es_unico=True)
        if not ok:
            return "MQTT_NOT_CONN"

        if random.random() < self.fault_cfg["dup_prob"]:
            self._inc("duplicated_extra")
            self._publicar(topic, payload_str, es_unico=False)
            return "DUPLICATED"

        return "MQTT_OK"

    def run(self):
        while not stop_event.is_set():
            self.tick += 1

            # Frenada periódica cada 30 ticks para producir alertas en ambas zonas RSU-1 y RSU-2
            if (self.tick % 30) in (8, 9):
                self.aceleracion = -6.0
            elif self.velocidad_ms < self.vel_base_ms:
                self.aceleracion = 3.0
            else:
                self.aceleracion = 0.0

            self.velocidad_ms = max(5.0, min(self.vel_base_ms, self.velocidad_ms + self.aceleracion * self.dt))
            self.posicion_x += self.velocidad_ms * self.dt

            # Al llegar al final del tramo (2000m), reentra por X=0m para mantener tráfico continuo
            if self.posicion_x >= HIGHWAY_LENGTH_M:
                self.posicion_x = round(self.posicion_x - HIGHWAY_LENGTH_M, 2)
                with print_lock:
                    print(f" 🔄 [REENTRADA] {self.veh_id} completó los 2000m y reentra al inicio del tramo (X={self.posicion_x}m)")

            pos_x = round(self.posicion_x, 2)
            vel_kmh = round(self.velocidad_ms * 3.6, 2)
            accel = round(self.aceleracion, 2)

            rsu_destino = "rsu-1" if self.posicion_x < 1000.0 else "rsu-2"
            if rsu_destino != self.rsu_actual:
                with print_lock:
                    print(f"\n 📡 [HANDOVER] {self.veh_id} cambió de celda (X={pos_x}m): {self.rsu_actual} ---> {rsu_destino}\n")
                self.rsu_actual = rsu_destino
                self._conectar_mqtt(self.rsu_actual)

            if accel <= BRAKING_THRESHOLD:
                event_type = "SUDDEN_BRAKING"
            else:
                event_type = "POSITION" if self.tick % 2 == 0 else "SPEED"

            evento = {
                "event_id": f"evt-{uuid.uuid4().hex[:10]}",
                "source_id": self.veh_id,
                "event_type": event_type,
                "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "location": {"posicion_x": pos_x},
                "payload": {"velocidad": vel_kmh, "aceleracion": accel},
            }

            topic = f"v2x/{self.rsu_actual}/telemetry"
            estado_envio = self._enviar_con_fallos(topic, json.dumps(evento))

            with print_lock:
                freno_str = " [!] FRENADA" if accel <= BRAKING_THRESHOLD else ""
                print(
                    f"[{estado_envio:<14}] [{self.rsu_actual.upper()}] {self.veh_id} | "
                    f"{event_type:<14} | X: {pos_x:>7.2f}m | V: {vel_kmh:>6.2f}km/h | A: {accel:>5.2f}m/s²{freno_str}"
                )

            stop_event.wait(self.dt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador V2X Lineal con Handover e Inyección de Fallos")
    parser.add_argument("--num-vehicles", type=int, default=5)
    parser.add_argument("--dt", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=60.0, help="Duración en segundos (0 = infinito hasta Ctrl+C)")
    parser.add_argument("--rsu1-host", default="localhost")
    parser.add_argument("--rsu1-port", type=int, default=1883)
    parser.add_argument("--rsu2-host", default="localhost")
    parser.add_argument("--rsu2-port", type=int, default=1884)
    parser.add_argument("--drop-prob", type=float, default=0.0)
    parser.add_argument("--dup-prob", type=float, default=0.0)
    parser.add_argument("--delay-prob", type=float, default=0.0)
    parser.add_argument("--max-delay", type=float, default=2.0)
    args = parser.parse_args()

    rsu_endpoints = {
        "rsu-1": (args.rsu1_host, args.rsu1_port),
        "rsu-2": (args.rsu2_host, args.rsu2_port),
    }
    fault_cfg = {
        "drop_prob": args.drop_prob,
        "dup_prob": args.dup_prob,
        "delay_prob": args.delay_prob,
        "max_delay": args.max_delay,
    }

    # Posiciones iniciales repartidas entre RSU-1 (400, 480, 560m) y RSU-2 (1200, 1280m)
    posiciones_base = [400.0, 480.0, 560.0, 1200.0, 1280.0]
    num_veh = max(5, args.num_vehicles)

    hilos = []
    for i in range(num_veh):
        pos_ini = posiciones_base[i] if i < len(posiciones_base) else (200.0 + (i * 150.0) % 1600.0)
        h = VehiculoOBU(f"veh-{i + 1:03d}", pos_inicial=pos_ini, vel_ms=32.0, dt=args.dt, rsu_endpoints=rsu_endpoints, fault_cfg=fault_cfg)
        hilos.append(h)
        h.start()

    inicio = time.monotonic()
    try:
        while True:
            if args.duration > 0 and (time.monotonic() - inicio) >= args.duration:
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for h in hilos:
            h.join(timeout=1.5)
        # Margen para que salgan los mensajes retardados y lleguen los PUBACK QoS1 antes de cerrar
        time.sleep(args.max_delay + 1.0 if args.delay_prob > 0 else 1.0)
        for h in hilos:
            h.cerrar()

        totales = {k: 0 for k in STAT_KEYS}
        por_vehiculo = {}
        print("\n=== RESUMEN DE ENVÍOS DEL SIMULADOR ===")
        for h in hilos:
            por_vehiculo[h.veh_id] = dict(h.stats)
            print(f" {h.veh_id}: {h.stats}")
            for k in totales:
                totales[k] += h.stats[k]

        print(f" TOTALES: {totales}")
        coherente = totales["generated"] == totales["dropped"] + totales["sent_unique"] + totales["publish_failed"]
        print(f" Comprobación generated == dropped + sent_unique + publish_failed -> {'OK' if coherente else 'DESCUADRE'}")
        with open("simulador_ground_truth.json", "w", encoding="utf-8") as f:
            json.dump({"timestamp": datetime.now(timezone.utc).isoformat(), "totals": totales, "per_vehicle": por_vehiculo}, f, indent=2)
