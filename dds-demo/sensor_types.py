"""Tipo de datos compartido (equivalente a un fichero IDL)."""
from dataclasses import dataclass
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.annotations import key
from cyclonedds.idl.types import int32, float64


@dataclass
class SensorReading(IdlStruct, typename="edge.SensorReading"):
    sensor_id: str          # clave -> cada sensor es una "instancia" distinta
    key("sensor_id")
    seq: int32
    temperature: float64
    timestamp: float64
