#!/usr/bin/env python3
"""Comprueba que Python y Cyclone DDS están listos."""
import sys
from importlib.metadata import version

if not (3, 10) <= sys.version_info[:2] <= (3, 13):
    sys.exit(f"ERROR: Python {sys.version.split()[0]} no es compatible (hace falta 3.10 - 3.13)")

from cyclonedds.domain import DomainParticipant

DomainParticipant(0)  # crea un participante real en el dominio 0
print(f"OK  Python {sys.version.split()[0]}  |  cyclonedds {version('cyclonedds')}  |  DDS funcionando")
