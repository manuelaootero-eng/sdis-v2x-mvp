import json
import unittest
from unittest.mock import MagicMock, patch

from v2x_logic import (
    ControladorCooldown,
    DeduplicadorEventos,
    DetectorHazard,
    MetricasV2X,
    clasificar_telemetria,
    validar_esquema_evento,
)
import consumidor_v2x
import rsu


class TestV2XSuite(unittest.TestCase):

    def test_clasificacion_estados_cinematicos(self):
        self.assertEqual(clasificar_telemetria(80.0, -6.0), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(80.0, -4.5), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(60.0, -2.0), "NORMAL_DECELERATION")
        self.assertEqual(clasificar_telemetria(90.0, 0.0), "CRUISE")
        self.assertEqual(clasificar_telemetria(0.0, 0.0), "STOPPED")
        self.assertEqual(clasificar_telemetria(40.0, 2.5), "ROUTINE_ACCELERATION")

    def test_validacion_esquema_basica(self):
        evento_valido = {
            "event_id": "evt-1234",
            "source_id": "veh-001",
            "event_type": "POSITION",
            "event_time": "2026-11-13T10:00:00Z",
            "location": {"posicion_x": 200.0},
            "payload": {"velocidad": 80.0, "aceleracion": 0.0},
        }
        self.assertTrue(validar_esquema_evento(evento_valido)[0])
        self.assertFalse(validar_esquema_evento({"event_id": "incompleto"})[0])
        self.assertFalse(validar_esquema_evento(dict(evento_valido, event_type="INVALID"))[0])
        self.assertFalse(validar_esquema_evento(dict(evento_valido, payload={"velocidad": "error", "aceleracion": 0}))[0])

    def test_detector_hazard_lineal(self):
        detector = DetectorHazard(time_window_s=5.0, max_distance_m=250.0)

        # Vehículos a >250m de distancia lineal (100m y 400m -> 300m) NO disparan peligro
        self.assertEqual(detector.registrar_frenada("veh-001", ts_unix=1000.0, posicion_x=100.0), [])
        self.assertEqual(detector.registrar_frenada("veh-002", ts_unix=1001.0, posicion_x=400.0), [])

        # Vehículo a 150m de veh-002 (550m vs 400m) dentro de <=5s SÍ dispara peligro
        self.assertEqual(
            detector.registrar_frenada("veh-003", ts_unix=1002.0, posicion_x=550.0),
            ["veh-002", "veh-003"],
        )

    def test_rsu_filtrado_edge_y_alerta_local(self):
        rsu.sqs_buffer.clear()
        rsu.deduplicador_mqtt = DeduplicadorEventos()
        rsu.cooldown_alerta_local = ControladorCooldown(cooldown_seconds=10.0)
        rsu.metricas = MetricasV2X()
        rsu.contador_rutinario_veh.clear()
        rsu.mqtt_client = MagicMock()

        # 3 mensajes rutinarios -> 2 filtrados en Edge, 1 encolado para SQS
        for i in range(1, 4):
            ev = {
                "event_id": f"evt-rut-{i}",
                "source_id": "veh-001",
                "event_type": "POSITION",
                "event_time": "2026-11-13T10:00:00Z",
                "location": {"posicion_x": 100.0 * i},
                "payload": {"velocidad": 80.0, "aceleracion": 0.0},
            }
            msg = MagicMock()
            msg.payload = json.dumps(ev).encode("utf-8")
            rsu.on_mqtt_message(None, None, msg)

        self.assertEqual(len(rsu.sqs_buffer), 1)
        self.assertEqual(rsu.metricas.contadores["filtered_edge"], 2)

        # Frenada brusca -> pasa directo a SQS y publica alerta MQTT local
        ev_freno = {
            "event_id": "evt-brk-1",
            "source_id": "veh-001",
            "event_type": "SUDDEN_BRAKING",
            "event_time": "2026-11-13T10:00:05Z",
            "location": {"posicion_x": 350.0},
            "payload": {"velocidad": 40.0, "aceleracion": -6.0},
        }
        msg = MagicMock()
        msg.payload = json.dumps(ev_freno).encode("utf-8")
        rsu.on_mqtt_message(None, None, msg)

        self.assertEqual(len(rsu.sqs_buffer), 2)
        self.assertEqual(rsu.metricas.contadores["alerts_triggered"], 1)

    def test_rsu_buffer_orden_y_reencolado_ante_fallo_sqs(self):
        rsu.sqs_buffer.clear()
        rsu.metricas = MetricasV2X()
        rsu.encolar_para_sqs({"event_id": "evt-1"})
        rsu.encolar_para_sqs({"event_id": "evt-2"})

        rsu.sqs_client = MagicMock()
        rsu.queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/v2x-events"
        rsu.sqs_client.send_message_batch.side_effect = RuntimeError("Network down")

        self.assertFalse(rsu.enviar_lote_sqs_una_vez())
        # El orden original [evt-1, evt-2] debe mantenerse intacto tras reencolar
        self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-1", "evt-2"])

    def test_consumidor_reintenta_si_falla_dynamodb(self):
        consumidor_v2x.deduplicador = DeduplicadorEventos()
        consumidor_v2x.ultimas_alertas.clear()
        evento = {
            "event_id": "evt-db-01",
            "source_id": "veh-001",
            "event_type": "SUDDEN_BRAKING",
            "event_time": "2026-11-13T10:00:00Z",
            "location": {"posicion_x": 300.0},
            "payload": {"velocidad": 50.0, "aceleracion": -6.0},
        }
        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", side_effect=RuntimeError("DB Error")):
            self.assertFalse(consumidor_v2x.procesar_evento(evento))

        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", return_value="STORED"), \
             patch.object(consumidor_v2x, "enviar_cloudwatch"), \
             patch.object(consumidor_v2x, "publicar_sns"):
            self.assertTrue(consumidor_v2x.procesar_evento(evento))
            self.assertEqual(len(consumidor_v2x.ultimas_alertas), 1)


if __name__ == "__main__":
    unittest.main()