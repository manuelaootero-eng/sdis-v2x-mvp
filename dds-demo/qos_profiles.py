"""Perfiles QoS usados en la demo."""
from cyclonedds.core import Qos, Policy
from cyclonedds.util import duration

# 1) Básico: fiable, solo el último dato
BASIC = Qos(
    Policy.Reliability.Reliable(max_blocking_time=duration(seconds=1)),
    Policy.History.KeepLast(1),
)

# 2) Durabilidad: el writer guarda las últimas 5 muestras para suscriptores tardíos
DURABLE = Qos(
    Policy.Reliability.Reliable(max_blocking_time=duration(seconds=1)),
    Policy.Durability.TransientLocal,
    Policy.History.KeepLast(5),
    # En Cyclone DDS la profundidad del histórico "transient local" la fija DurabilityService
    Policy.DurabilityService(cleanup_delay=0, history=Policy.History.KeepLast(5),
                             max_samples=-1, max_instances=-1, max_samples_per_instance=-1),
)

# 3) Deadline: se espera una muestra cada 1 s como máximo
def deadline(seconds: float) -> Qos:
    return Qos(
        Policy.Reliability.Reliable(max_blocking_time=duration(seconds=1)),
        Policy.History.KeepLast(1),
        Policy.Deadline(duration(seconds=seconds)),
    )

PROFILES = {"basic": BASIC, "durable": DURABLE}
