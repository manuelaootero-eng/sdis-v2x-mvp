#!/usr/bin/env bash
# Instalación para Ubuntu y macOS. Uso:  bash setup.sh
set -e
cd "$(dirname "$0")"

PY=""
# cyclonedds solo publica binarios para Python 3.10 - 3.13
for c in python3.12 python3.13 python3.11 python3.10 python3; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(0 if (3,10) <= sys.version_info[:2] <= (3,13) else 1)'; then
    PY="$c"; break
  fi
done
if [ -z "$PY" ]; then
  echo "ERROR: hace falta Python entre 3.10 y 3.13 (recomendado 3.12). Ver paso 1 de la guía"; exit 1
fi
echo "Usando $($PY --version)"

rm -rf .venv
"$PY" -m venv .venv || { echo "ERROR creando el entorno. En Ubuntu: sudo apt install python3-venv"; exit 1; }
.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet cyclonedds
.venv/bin/python check_install.py
echo "Listo. En cada terminal: source .venv/bin/activate"
