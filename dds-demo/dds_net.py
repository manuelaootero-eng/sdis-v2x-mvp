"""Configuración de red opcional: si existe cyclonedds.xml en esta carpeta, se aplica.
(Solo hace falta si los procesos no se ven: VPN, Docker, varias tarjetas de red...)"""
import os
from pathlib import Path

_cfg = Path(__file__).with_name("cyclonedds.xml")
if _cfg.exists() and "CYCLONEDDS_URI" not in os.environ:
    os.environ["CYCLONEDDS_URI"] = _cfg.read_text(encoding="utf-8")
    print(f"[red] usando {_cfg.name}")
