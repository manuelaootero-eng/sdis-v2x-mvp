# SDIS · Sistema V2X Edge-to-Cloud en AWS

Sistema distribuido V2X de tres niveles (OBU → RSU Edge → V2X-SERVICE en la nube) sobre AWS Academy (`us-east-1`).
Autoras: Manuela Outeiro Otero y Eva Deibe de Bernardo · Sistemas Distribuidos (Máster en Ingeniería de Telecomunicación).


## Contenido

| Ruta | Función |
|---|---|
| `productor_v2x.py` | Simulador de vehículos (OBU) con inyección de pérdida, duplicación y retardo |
| `rsu.py` | RSU (Edge): MQTT, validación, alerta local, filtrado, buffer y envío a SQS, receptor de SNS |
| `consumidor_v2x.py` | V2X-SERVICE: consume SQS, guarda en DynamoDB, genera alertas, API REST |
| `v2x_logic.py` | Lógica compartida (esquema, deduplicación, detector de peligro, métricas) |
| `test_eventos.py` | 17 pruebas unitarias |
| `Dockerfile.rsu`, `Dockerfile.service`, `docker_compose.yml` | Imágenes y ejecución local |
| `.github/workflows/deploy-mvp.yml` | CI/CD: tests → build → ECR → despliegue por SSM |
| `experiments/` | Herramientas y resultados de los experimentos (ver abajo) |

## Tests

```bash
pip install -r requirements.txt
python3 -m unittest test_eventos.py -v     # 17 tests, OK (los del simulador tardan ~20 s)
```

## Despliegue en AWS

Recursos (consola web o CLI), creados una sola vez:

- **ECR:** repositorio `sdis-v2x-mvp`.
- **SQS:** cola Standard `v2x-events` (visibility timeout 30 s, retención 4 días, DLQ `v2x-events-dlq` con `maxReceiveCount` 5).
- **DynamoDB** (bajo demanda): `v2x-events-store` (clave `event_id`) y `v2x-alerts-store` (clave `alert_id`).
- **3 EC2** con Docker, agente SSM y perfil de instancia con permisos sobre SQS, SNS, DynamoDB, CloudWatch Logs, ECR y `ec2:DescribeTags`. Etiquetas: `V2XApp=true` y `Name` (o `Role`) que contenga `RSU-1`, `RSU-2` o `SERVICE`.
- **Puertos:** 80 y 1883 en los RSU, 8080 en el servicio.
- **Secretos de GitHub:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (caducan con cada sesión de AWS Academy).
- **SNS:** el servicio crea el tópico `v2x-alerts`; suscribir ambos RSU por HTTP a `http://<IP_RSU>:80/sns-notifications` (se confirman solos).

Un `push` a `main` ejecuta el pipeline y despliega en las EC2 las imágenes `rsu-<sha>` y `service-<sha>`.
Comprobación: `curl http://<IP_RSU1>/health`, `http://<IP_RSU2>/health` y `http://<IP_SERVICIO>:8080/health`.

## Ejecutar el simulador

```bash
python3 productor_v2x.py --num-vehicles 5 --dt 1.0 --duration 60 \
  --rsu1-host <IP_RSU1> --rsu1-port 1883 --rsu2-host <IP_RSU2> --rsu2-port 1883
```

Fallos opcionales: `--drop-prob`, `--dup-prob`, `--delay-prob`, `--max-delay`. Escribe `simulador_ground_truth.json`.
En local con Docker Compose: `docker compose -f docker_compose.yml up --build -d` (RSU-2 en el puerto 1884).

## Experimentos

`experiments/exp.py` (solo HTTP; editar las IP al principio del fichero) y `experiments/limpiar_dynamo.py`. Se ejecutan desde la raíz del repo; los resultados se guardan en `experiments/results/`.

1. **Preparar:** purgar la cola SQS (consola, esperar 60 s), `python3 experiments/limpiar_dynamo.py` y `python3 experiments/exp.py reset`.
2. **Ejecutar** el simulador (ver tabla).
3. **Recoger** (15 s después): `python3 experiments/exp.py recoger <nombre>`.

| Experimento | Ejecución |
|---|---|
| E0 línea base | simulador con `--duration 60` |
| E1 duplicados | `--duration 60` con `--dup-prob 0.1` y con `0.4` |
| E2 desconexión Edge-Cloud | `--duration 120` y `python3 experiments/exp.py monitor http://<IP_RSU1>/health e2_buffer.log`. En la EC2 del RSU-1, a los 20 s: `sudo iptables -I DOCKER-USER -p tcp --dport 443 -j DROP`; 30 s después: `sudo iptables -D DOCKER-USER -p tcp --dport 443 -j DROP` |
| E3 caída del servicio | `--duration 120` y `python3 experiments/exp.py cola e3_cola.log`. En la EC2 del servicio, a los 20 s: `sudo docker stop v2x-service`; 40 s después: `sudo docker start v2x-service`. Ctrl+C al vaciarse la cola; usar el N1 del resumen en `recoger e3 <N1>` |

Perdidos OBU→RSU = `sent_unique − (accepted_RSU1 + accepted_RSU2)`; perdidos tras RSU = `Σ_RSU (accepted − filtered_edge − dropped_buffer) − (N1 − N0)`.

**Resultados en bruto** (`experiments/results/`): `e0.json`, `e1_01.json`, `e1_04.json`, `e2.json`, `e3.json`, `e2_buffer.log`, `e3_cola.log`.
Notas: `e2.json` acumula los contadores de dos ejecuciones consecutivas (valores de E2 de la memoria = diferencia con `e3.json` anterior); `e2_buffer.log` contiene dos ejecuciones y la válida es la de las 14:07 UTC (corte 14:07:33–14:08:03).

## Teardown (tras la defensa)

Terminar las 3 EC2 y eliminar las tablas DynamoDB, la cola `v2x-events` (y su DLQ), el tópico `v2x-alerts`, el grupo de logs `/v2x-mvp/alerts` y el repositorio ECR `sdis-v2x-mvp`.
