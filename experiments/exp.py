"""Herramienta local para los experimentos. Solo hace peticiones HTTP; no usa AWS.
Ejecutar desde la raíz del repositorio (donde el simulador deja simulador_ground_truth.json).
Los ficheros de salida se guardan en experiments/results/.

  python3 experiments/exp.py reset                      -> pone a cero los contadores de RSU-1, RSU-2 y servicio
  python3 experiments/exp.py recoger <nombre> [N1]      -> descarga métricas, calcula resultados y guarda <nombre>.json
                                               (si no se indica N1, usa los eventos aceptados por el servicio)
  python3 experiments/exp.py monitor <url> <fichero>    -> consulta una URL cada 2 s con hora UTC (Ctrl+C para parar)
  python3 experiments/exp.py cola <fichero>             -> E3: mide pico de cola y tiempo de recuperación (Ctrl+C al final)
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

RSU1 = "http://52.207.249.191"
RSU2 = "http://34.229.94.60"
SERVICE = "http://34.226.195.151:8080"


RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")


def ruta(nombre):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return os.path.join(RESULTS_DIR, nombre)


def get(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def post(url):
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.read().decode()


def reset():
    for base in (RSU1, RSU2, SERVICE):
        print(base, post(base + "/metrics/reset"))


def recoger(nombre, n1=None):
    r1, r2, sv = get(RSU1 + "/metrics"), get(RSU2 + "/metrics"), get(SERVICE + "/metrics")
    gt = json.load(open("simulador_ground_truth.json"))["totals"]
    if n1 is None:
        n1 = sv["counters"]["accepted"]
        print("(N1 = eventos aceptados y guardados por el servicio)")
    json.dump({"ground_truth": gt, "rsu1": r1, "rsu2": r2, "servicio": sv, "N1": n1},
              open(ruta(f"{nombre}.json"), "w"), indent=2)
    c = [r1["counters"], r2["counters"]]
    aceptados = sum(x["accepted"] for x in c)
    filtrados = sum(x["filtered_edge"] for x in c)
    descartados_buf = sum(x["dropped_buffer"] for x in c)
    a_sqs = aceptados - filtrados - descartados_buf
    print(f"Generados: {gt['generated']} | enviados únicos (sent_unique): {gt['sent_unique']} | duplicados inyectados: {gt['duplicated_extra']}")
    print(f"Aceptados RSU: {aceptados} | filtrados Edge: {filtrados} | enviados a SQS (esperados en BD): {a_sqs}")
    print(f"Duplicados descartados en RSU: {sum(x['duplicates'] for x in c)} | en servicio: {sv['counters']['duplicates']}")
    print(f"dropped_buffer: {descartados_buf} | persistidos (N1): {n1}")
    print(f"Perdidos OBU->RSU: {gt['sent_unique'] - aceptados} | perdidos tras RSU: {a_sqs - n1}")
    print(f"Consistencia final: {'SI' if a_sqs == n1 else 'NO'}")
    print(f"p95 RSU-1: {r1['latency_p95_ms']} ms | p95 RSU-2: {r2['latency_p95_ms']} ms | p95 servicio: {sv['latency_p95_ms']} ms")
    print(f"Guardado en {ruta(nombre + '.json')}")


def monitor(url, salida):
    with open(ruta(salida), "a") as f:
        while True:
            t = datetime.now(timezone.utc).strftime("%H:%M:%S")
            try:
                dato = json.dumps(get(url))
            except Exception as e:
                dato = f"ERROR {e}"
            linea = f"{t} UTC {dato}"
            print(linea)
            f.write(linea + "\n")
            f.flush()
            time.sleep(2)


def cola(salida, umbral=5):
    """Cola estimada = eventos enviados a SQS por los RSU - eventos guardados por el servicio.
    El contador del servicio vuelve a 0 al reiniciarse; se acumula lo anterior para no perderlo."""
    base, total, activo_antes = 0, 0, True
    t_caida = t_vuelta = t_pico = t_rec = None
    pico, fwd = 0, 0
    f = open(ruta(salida), "a")
    try:
        while True:
            ahora = datetime.now(timezone.utc)
            try:
                fwd = sum(get(b + "/metrics")["counters"]["forwarded_or_persisted"] for b in (RSU1, RSU2))
            except Exception:
                time.sleep(2)
                continue
            try:
                acc = get(SERVICE + "/metrics")["counters"]["accepted"]
                activo = True
            except Exception:
                activo = False
            if activo:
                if not activo_antes:          # el servicio acaba de volver: su contador empieza de nuevo
                    base = total
                    t_vuelta = t_vuelta or ahora
                total = base + acc
            elif t_caida is None:
                t_caida = ahora
            activo_antes = activo
            backlog = fwd - total
            if backlog > pico:
                pico, t_pico = backlog, ahora
            if t_vuelta and t_rec is None and backlog <= umbral:
                t_rec = ahora
            linea = (f"{ahora.strftime('%H:%M:%S')} UTC | enviados_a_SQS={fwd} guardados={total} "
                     f"cola_estimada={backlog} servicio={'OK' if activo else 'CAIDO'}")
            print(linea)
            f.write(linea + "\n")
            f.flush()
            time.sleep(2)
    except KeyboardInterrupt:
        fmt = lambda t: t.strftime("%H:%M:%S") if t else "no observado"
        print("\n=== RESUMEN E3 ===")
        print(f"Servicio caído desde: {fmt(t_caida)} | vuelve a responder: {fmt(t_vuelta)}")
        print(f"Pico de cola estimada: {pico} mensajes ({fmt(t_pico)})")
        if t_vuelta and t_rec:
            print(f"Tiempo de recuperación: {(t_rec - t_vuelta).total_seconds():.0f} s (cola <= {umbral} tras volver el servicio)")
        else:
            print("Tiempo de recuperación: no observado (¿se paró el monitor antes de vaciarse la cola?)")
        print(f"Total enviados a SQS: {fwd} | total guardados por el servicio (usar como N1): {total} | diferencia: {fwd - total}")
        f.write(f"RESUMEN pico={pico} caida={fmt(t_caida)} vuelta={fmt(t_vuelta)} recuperado={fmt(t_rec)} N1={total}\n")
        f.close()


if __name__ == "__main__":
    a = sys.argv
    if len(a) >= 2 and a[1] == "reset":
        reset()
    elif len(a) in (3, 4) and a[1] == "recoger":
        recoger(a[2], int(a[3]) if len(a) == 4 else None)
    elif len(a) == 4 and a[1] == "monitor":
        monitor(a[2], a[3])
    elif len(a) == 3 and a[1] == "cola":
        cola(a[2])
    else:
        print(__doc__)
