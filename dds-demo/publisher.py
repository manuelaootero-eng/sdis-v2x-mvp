#!/usr/bin/env python3
"""Publicador: simula un sensor de temperatura en un nodo Edge."""
import dds_net  # noqa: F401  (config. de red opcional, antes de crear entidades DDS)
import argparse, os, random, time
from cyclonedds.domain import DomainParticipant
from cyclonedds.topic import Topic
from cyclonedds.pub import DataWriter
from sensor_types import SensorReading
from qos_profiles import PROFILES, deadline

p = argparse.ArgumentParser()
p.add_argument("--qos", choices=["basic", "durable", "deadline"], default="basic")
p.add_argument("--id", default="sensor-1")
p.add_argument("--period", type=float, default=0.5, help="segundos entre muestras")
p.add_argument("--count", type=int, default=0, help="0 = infinito")
p.add_argument("--crash-after", type=int, default=0,
               help="simula un fallo abrupto tras N muestras (sin cerrar DDS)")
args = p.parse_args()

qos = deadline(1.0) if args.qos == "deadline" else PROFILES[args.qos]

dp = DomainParticipant(0)                         # dominio DDS 0
topic = Topic(dp, "Temperature", SensorReading)   # mismo nombre y tipo que el subscriber
writer = DataWriter(dp, topic, qos=qos)

print(f"[PUB {args.id}] QoS={args.qos}, periodo={args.period}s")
seq = 0
try:
    while args.count == 0 or seq < args.count:
        seq += 1
        s = SensorReading(args.id, seq, round(20 + random.uniform(-2, 2), 2), time.time())
        writer.write(s)
        print(f"[PUB {args.id}] seq={s.seq:3d}  T={s.temperature:.2f} °C")
        time.sleep(args.period)
        if args.crash_after and seq >= args.crash_after:
            print("[PUB] *** fallo simulado: el proceso muere sin cerrar DDS ***", flush=True)
            os._exit(1)
    # con --count, el writer sigue vivo al terminar (para probar suscriptores tardíos)
    print("[PUB] envío terminado; el writer sigue vivo (Ctrl+C para salir)")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
