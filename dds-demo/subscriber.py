#!/usr/bin/env python3
"""Suscriptor: recibe lecturas de temperatura (p. ej. un servicio de analítica Edge)."""
import dds_net  # noqa: F401  (config. de red opcional, antes de crear entidades DDS)
import argparse, time
from cyclonedds.domain import DomainParticipant
from cyclonedds.topic import Topic
from cyclonedds.sub import DataReader
from cyclonedds.core import Listener, WaitSet, ReadCondition, ViewState, InstanceState, SampleState
from cyclonedds.util import duration
from sensor_types import SensorReading
from qos_profiles import PROFILES, deadline

p = argparse.ArgumentParser()
p.add_argument("--qos", choices=["basic", "durable", "deadline"], default="basic")
args = p.parse_args()


POLICY_NAMES = {2: "DURABILITY", 4: "DEADLINE", 8: "LIVELINESS", 11: "RELIABILITY", 13: "HISTORY"}


class Alarms(Listener):
    def on_requested_deadline_missed(self, reader, status):
        print(f"  !! DEADLINE PERDIDO (total={status.total_count}) -> sensor caído o red lenta")

    def on_requested_incompatible_qos(self, reader, status):
        name = POLICY_NAMES.get(status.last_policy_id, status.last_policy_id)
        print(f"  !! QoS INCOMPATIBLE con un writer (política: {name}) -> no hay comunicación")

    def on_subscription_matched(self, reader, status):
        print(f"  -- writers emparejados: {status.current_count}")


qos = deadline(1.0) if args.qos == "deadline" else PROFILES[args.qos]

dp = DomainParticipant(0)
topic = Topic(dp, "Temperature", SensorReading)
reader = DataReader(dp, topic, qos=qos, listener=Alarms())

cond = ReadCondition(reader, ViewState.Any | InstanceState.Any | SampleState.NotRead)  # datos nuevos
ws = WaitSet(dp)
ws.attach(cond)

print(f"[SUB] QoS={args.qos}, esperando datos...")
try:
    while True:
        ws.wait(duration(milliseconds=200))
        for s in reader.take(N=20):
            if not hasattr(s, "seq"):      # muestras sin datos (p. ej. dispose)
                continue
            lat_ms = (time.time() - s.timestamp) * 1000
            print(f"[SUB] {s.sensor_id} seq={s.seq:3d}  T={s.temperature:.2f} °C  "
                  f"latencia={lat_ms:.2f} ms")
except KeyboardInterrupt:
    pass
