import json
import time
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
import productor_v2x
import rsu


def evento_base(**cambios):
    ev = {
        "event_id": "evt-1234",
        "source_id": "veh-001",
        "event_type": "POSITION",
        "event_time": "2026-11-13T10:00:00Z",
        "location": {"posicion_x": 200.0},
        "payload": {"velocidad": 80.0, "aceleracion": 0.0},
    }
    ev.update(cambios)
    return ev


def mensaje_mqtt(evento: dict):
    msg = MagicMock()
    msg.payload = json.dumps(evento).encode("utf-8")
    return msg


class TestLogica(unittest.TestCase):

    def test_clasificacion_estados_cinematicos(self):
        self.assertEqual(clasificar_telemetria(80.0, -6.0), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(80.0, -4.5), "SUDDEN_BRAKING")
        self.assertEqual(clasificar_telemetria(60.0, -2.0), "NORMAL_DECELERATION")
        self.assertEqual(clasificar_telemetria(90.0, 0.0), "CRUISE")
        self.assertEqual(clasificar_telemetria(0.0, 0.0), "STOPPED")
        self.assertEqual(clasificar_telemetria(40.0, 2.5), "ROUTINE_ACCELERATION")

    def test_validacion_esquema_basica(self):
        self.assertTrue(validar_esquema_evento(evento_base())[0])
        self.assertFalse(validar_esquema_evento({"event_id": "incompleto"})[0])
        self.assertFalse(validar_esquema_evento(evento_base(event_type="INVALID"))[0])
        self.assertFalse(validar_esquema_evento(evento_base(payload={"velocidad": "error", "aceleracion": 0}))[0])

    def test_validacion_rechaza_ids_no_string_y_valores_no_finitos(self):
        self.assertFalse(validar_esquema_evento(evento_base(event_id=["x"]))[0])
        self.assertFalse(validar_esquema_evento(evento_base(event_id="   "))[0])
        self.assertFalse(validar_esquema_evento(evento_base(event_type=["POSITION"]))[0])
        self.assertFalse(validar_esquema_evento(evento_base(location=["x"]))[0])
        self.assertFalse(validar_esquema_evento(evento_base(payload={"velocidad": float("nan"), "aceleracion": 0.0}))[0])
        self.assertFalse(validar_esquema_evento(evento_base(payload={"velocidad": 80.0, "aceleracion": float("inf")}))[0])

    def test_deduplicador_y_olvidar(self):
        dedup = DeduplicadorEventos()
        self.assertFalse(dedup.es_duplicado("evt-a"))
        self.assertTrue(dedup.es_duplicado("evt-a"))
        dedup.olvidar("evt-a")
        self.assertFalse(dedup.es_duplicado("evt-a"))

    def test_detector_hazard_lineal(self):
        detector = DetectorHazard(time_window_s=5.0, max_distance_m=250.0)

        # Vehículos a >250m (100m y 400m -> 300m) NO disparan peligro
        self.assertEqual(detector.registrar_frenada("veh-001", ts_unix=1000.0, posicion_x=100.0), [])
        self.assertEqual(detector.registrar_frenada("veh-002", ts_unix=1001.0, posicion_x=400.0), [])

        # Vehículo a 150m de veh-002 (550m vs 400m) dentro de <=5s SÍ dispara peligro
        self.assertEqual(
            detector.registrar_frenada("veh-003", ts_unix=1002.0, posicion_x=550.0),
            ["veh-002", "veh-003"],
        )

    def test_detector_hazard_no_repite_alerta_del_mismo_incidente(self):
        d = DetectorHazard(time_window_s=5.0, max_distance_m=250.0, cooldown_s=10.0)
        d.registrar_frenada("veh-001", 100.0, 400.0)
        self.assertEqual(d.registrar_frenada("veh-002", 100.0, 480.0), ["veh-001", "veh-002"])

        # Un tick después siguen frenando: es el mismo incidente -> sin nueva alerta
        d.registrar_frenada("veh-001", 101.0, 430.0)
        self.assertEqual(d.registrar_frenada("veh-002", 101.0, 510.0), [])

        # Pasado el cooldown, un nuevo incidente vuelve a disparar
        d.registrar_frenada("veh-001", 130.0, 700.0)
        self.assertEqual(d.registrar_frenada("veh-002", 130.0, 780.0), ["veh-001", "veh-002"])


class TestRSU(unittest.TestCase):

    def setUp(self):
        rsu.sqs_buffer.clear()
        rsu.deduplicador_mqtt = DeduplicadorEventos()
        rsu.deduplicador_sns = DeduplicadorEventos()
        rsu.cooldown_alerta_local = ControladorCooldown(cooldown_seconds=10.0)
        rsu.metricas = MetricasV2X()
        rsu.contador_rutinario_veh.clear()
        rsu.mqtt_client = MagicMock()

    def test_filtrado_edge_y_alerta_local(self):
        # 3 mensajes rutinarios -> 2 filtrados en el edge, 1 encolado para SQS
        for i in range(1, 4):
            rsu.on_mqtt_message(None, None, mensaje_mqtt(evento_base(event_id=f"evt-rut-{i}")))
        self.assertEqual(len(rsu.sqs_buffer), 1)
        self.assertEqual(rsu.metricas.contadores["filtered_edge"], 2)

        # Frenada brusca -> pasa directo a SQS y publica alerta MQTT local
        freno = evento_base(event_id="evt-brk-1", event_type="SUDDEN_BRAKING", payload={"velocidad": 40.0, "aceleracion": -6.0})
        rsu.on_mqtt_message(None, None, mensaje_mqtt(freno))
        self.assertEqual(len(rsu.sqs_buffer), 2)
        self.assertEqual(rsu.metricas.contadores["alerts_triggered"], 1)

    def test_duplicado_mqtt_se_descarta_en_el_edge(self):
        ev = evento_base(event_id="evt-dup")
        rsu.on_mqtt_message(None, None, mensaje_mqtt(ev))
        rsu.on_mqtt_message(None, None, mensaje_mqtt(ev))
        self.assertEqual(rsu.metricas.contadores["received"], 2)
        self.assertEqual(rsu.metricas.contadores["duplicates"], 1)
        self.assertEqual(rsu.metricas.contadores["accepted"], 1)

    def test_callback_no_propaga_excepciones(self):
        msg = MagicMock()
        msg.payload = b"\xff\xfe no es json"
        rsu.on_mqtt_message(None, None, msg)  # no debe lanzar

    def test_buffer_orden_y_reencolado_ante_fallo_sqs(self):
        rsu.encolar_para_sqs({"event_id": "evt-1"})
        rsu.encolar_para_sqs({"event_id": "evt-2"})

        rsu.sqs_client = MagicMock()
        rsu.queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/v2x-events"
        rsu.sqs_client.send_message_batch.side_effect = RuntimeError("Network down")

        self.assertFalse(rsu.enviar_lote_sqs_una_vez())
        # El orden original [evt-1, evt-2] debe mantenerse intacto tras reencolar
        self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-1", "evt-2"])

    def test_buffer_drop_oldest(self):
        with patch.object(rsu, "MAX_BUFFER_SIZE", 3):
            for i in range(1, 5):
                rsu.encolar_para_sqs({"event_id": f"evt-{i}"})
            self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-2", "evt-3", "evt-4"])
            self.assertEqual(rsu.metricas.contadores["dropped_buffer"], 1)

            # Reencolar eventos más antiguos con el buffer lleno: se descartan ellos, no los nuevos
            rsu._reencolar_frente([{"event_id": "evt-0"}])
            self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-2", "evt-3", "evt-4"])
            self.assertEqual(rsu.metricas.contadores["dropped_buffer"], 2)

    def test_lote_totalmente_fallido_devuelve_false_y_reencola(self):
        rsu.encolar_para_sqs({"event_id": "evt-1"})
        rsu.sqs_client = MagicMock()
        rsu.queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/v2x-events"
        rsu.sqs_client.send_message_batch.return_value = {"Successful": [], "Failed": [{"Id": "msg-0"}]}

        self.assertFalse(rsu.enviar_lote_sqs_una_vez())
        self.assertEqual([e["event_id"] for e in rsu.sqs_buffer], ["evt-1"])

    def test_sns_confirmacion_reintentable_y_notificacion_deduplicada(self):
        cli = rsu.app.test_client()
        sub = {"Type": "SubscriptionConfirmation", "MessageId": "m-sub", "SubscribeURL": "http://example.com/c"}

        with patch.object(rsu.requests, "get", side_effect=rsu.requests.RequestException("timeout")):
            self.assertEqual(cli.post("/sns-notifications", json=sub).status_code, 500)
        with patch.object(rsu.requests, "get") as mock_get:
            self.assertEqual(cli.post("/sns-notifications", json=sub).status_code, 200)
            self.assertEqual(mock_get.call_count, 1)

        notif = {"Type": "Notification", "MessageId": "m-1", "Message": "alerta global"}
        cli.post("/sns-notifications", json=notif)
        cli.post("/sns-notifications", json=notif)
        self.assertEqual(rsu.mqtt_client.publish.call_count, 1)


class TestServicio(unittest.TestCase):

    def test_reintenta_si_falla_dynamodb(self):
        consumidor_v2x.deduplicador = DeduplicadorEventos()
        consumidor_v2x.ultimas_alertas.clear()
        evento = evento_base(
            event_id="evt-db-01",
            event_type="SUDDEN_BRAKING",
            location={"posicion_x": 300.0},
            payload={"velocidad": 50.0, "aceleracion": -6.0},
        )
        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", side_effect=RuntimeError("DB Error")):
            self.assertFalse(consumidor_v2x.procesar_evento(evento))

        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", return_value="STORED"), \
             patch.object(consumidor_v2x, "enviar_cloudwatch"), \
             patch.object(consumidor_v2x, "publicar_sns"):
            self.assertTrue(consumidor_v2x.procesar_evento(evento))
            self.assertEqual(len(consumidor_v2x.ultimas_alertas), 1)

    def test_duplicado_en_dynamodb_no_genera_alertas(self):
        consumidor_v2x.deduplicador = DeduplicadorEventos()
        consumidor_v2x.ultimas_alertas.clear()
        evento = evento_base(
            event_id="evt-dup-db",
            event_type="SUDDEN_BRAKING",
            payload={"velocidad": 50.0, "aceleracion": -6.0},
        )
        with patch.object(consumidor_v2x, "guardar_evento_dynamodb", return_value="DUPLICATE"), \
             patch.object(consumidor_v2x, "publicar_sns") as sns_mock:
            self.assertTrue(consumidor_v2x.procesar_evento(evento))
            sns_mock.assert_not_called()
        self.assertEqual(len(consumidor_v2x.ultimas_alertas), 0)


class TestSimulador(unittest.TestCase):

    ENDPOINTS = {"rsu-1": ("h", 1883), "rsu-2": ("h", 1884)}

    def nuevo_vehiculo(self, **fallos):
        cfg = {"drop_prob": 0.0, "dup_prob": 0.0, "delay_prob": 0.0, "max_delay": 2.0}
        cfg.update(fallos)
        with patch.object(productor_v2x.mqtt, "Client") as cliente_cls:
            cliente_cls.return_value.publish.return_value.rc = productor_v2x.mqtt.MQTT_ERR_SUCCESS
            return productor_v2x.VehiculoOBU("veh-001", 100.0, 32.0, 1.0, self.ENDPOINTS, cfg)

    def test_contabilidad_de_envios(self):
        obu = self.nuevo_vehiculo(drop_prob=1.0)
        self.assertEqual(obu._enviar_con_fallos("t", "p"), "DROPPED")
        self.assertEqual((obu.stats["generated"], obu.stats["dropped"], obu.stats["sent_unique"]), (1, 1, 0))

        obu = self.nuevo_vehiculo(dup_prob=1.0)
        self.assertEqual(obu._enviar_con_fallos("t", "p"), "DUPLICATED")
        self.assertEqual(
            (obu.stats["sent_unique"], obu.stats["duplicated_extra"], obu.stats["mqtt_publishes"]), (1, 1, 2)
        )

        obu = self.nuevo_vehiculo()
        obu.mqtt_client.publish.return_value.rc = productor_v2x.mqtt.MQTT_ERR_NO_CONN
        self.assertEqual(obu._enviar_con_fallos("t", "p"), "MQTT_NOT_CONN")
        self.assertEqual((obu.stats["sent_unique"], obu.stats["publish_failed"]), (0, 1))

    def test_mensaje_retardado_sale_por_el_cliente_original_tras_handover(self):
        obu = self.nuevo_vehiculo(delay_prob=1.0, max_delay=0.2)
        cliente_rsu1 = obu.mqtt_client
        self.assertTrue(obu._enviar_con_fallos("v2x/rsu-1/telemetry", "p").startswith("DELAYED"))

        obu.mqtt_client = MagicMock()  # simula el handover: ahora hay otro cliente
        time.sleep(0.5)

        cliente_rsu1.publish.assert_called_once()
        obu.mqtt_client.publish.assert_not_called()
        self.assertEqual(obu.stats["sent_unique"], 1)


if __name__ == "__main__":
    unittest.main()
