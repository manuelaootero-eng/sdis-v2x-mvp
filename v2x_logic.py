import collections
import threading
import time

REQUIRED_FIELDS = {"event_id", "source_id", "event_type", "event_time", "location", "payload"}
VALID_EVENT_TYPES = {"POSITION", "SPEED", "SUDDEN_BRAKING"}
BRAKING_THRESHOLD = -4.5
HIGHWAY_LENGTH_M = 2000.0


def clasificar_telemetria(velocidad_kmh: float, aceleracion_ms2: float) -> str:
    """Clasifica el estado físico según aceleración y velocidad."""
    if aceleracion_ms2 <= BRAKING_THRESHOLD:
        return "SUDDEN_BRAKING"
    if aceleracion_ms2 < -0.05:
        return "NORMAL_DECELERATION"
    if abs(aceleracion_ms2) <= 0.05:
        return "STOPPED" if velocidad_kmh <= 0.5 else "CRUISE"
    return "ROUTINE_ACCELERATION"


def validar_esquema_evento(evento: dict) -> tuple[bool, str]:
    """Verifica campos obligatorios y formato numérico básico."""
    if not isinstance(evento, dict) or not REQUIRED_FIELDS.issubset(evento.keys()):
        return False, "Faltan campos obligatorios"

    if evento.get("event_type") not in VALID_EVENT_TYPES:
        return False, "event_type inválido"

    try:
        float(evento["location"]["posicion_x"])
        float(evento["payload"]["velocidad"])
        float(evento["payload"]["aceleracion"])
    except (KeyError, ValueError, TypeError):
        return False, "Formato numérico inválido en location o payload"

    return True, "OK"


class DeduplicadorEventos:
    """Control de duplicados en memoria con limpieza periódica cada cleanup_interval."""
    def __init__(self, ttl_seconds: float = 300.0, cleanup_interval: float = 15.0):
        self.ttl = ttl_seconds
        self.cleanup_interval = cleanup_interval
        self.vistos = {}
        self.ultimo_cleanup = 0.0
        self.lock = threading.Lock()

    def es_duplicado(self, event_id: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            if now - self.ultimo_cleanup >= self.cleanup_interval:
                self.vistos = {k: ts for k, ts in self.vistos.items() if now - ts <= self.ttl}
                self.ultimo_cleanup = now
            if event_id in self.vistos:
                return True
            self.vistos[event_id] = now
            return False


class ControladorCooldown:
    """Ventana de enfriamiento por clave para evitar tormentas de alertas."""
    def __init__(self, cooldown_seconds: float = 8.0):
        self.cooldown = cooldown_seconds
        self.ultimos = {}
        self.lock = threading.Lock()

    def puede_disparar(self, clave: str, now: float = None) -> bool:
        now = time.time() if now is None else now
        with self.lock:
            ultimo = self.ultimos.get(clave)
            if ultimo is not None and (now - ultimo) < self.cooldown:
                return False
            self.ultimos[clave] = now
            return True


class DetectorHazard:
    """
    Detecta peligro colectivo (HAZARD_DETECTED) cuando 2 o más vehículos frenan
    a una distancia lineal abs(pos1 - pos2) <= 250m dentro de una ventana de 5s.
    """
    def __init__(self, time_window_s: float = 5.0, max_distance_m: float = 250.0):
        self.time_window_s = time_window_s
        self.max_distance_m = max_distance_m
        self.frenadas = {}  # veh_id -> (ts_unix, posicion_x)
        self.lock = threading.Lock()

    def registrar_frenada(self, source_id: str, ts_unix: float, posicion_x: float) -> list[str]:
        pos_x = float(posicion_x)
        with self.lock:
            self.frenadas = {
                vid: (t, p) for vid, (t, p) in self.frenadas.items()
                if abs(ts_unix - t) <= self.time_window_s
            }
            self.frenadas[source_id] = (ts_unix, pos_x)

            implicados = [
                vid for vid, (t, pos) in self.frenadas.items()
                if abs(pos_x - pos) <= self.max_distance_m
            ]

            if len(implicados) >= 2:
                implicados.sort()
                for vid in implicados:
                    self.frenadas.pop(vid, None)
                return implicados
            return []


class MetricasV2X:
    """Contadores de negocio (incluyendo dropped_buffer) y latencia p95."""
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.contadores = {
                "received": 0,
                "accepted": 0,
                "processed": 0,
                "duplicates": 0,
                "filtered_edge": 0,
                "dropped_buffer": 0,
                "forwarded_or_persisted": 0,
                "alerts_triggered": 0,
            }
            self.latencias_ms = collections.deque(maxlen=2000)

    def inc(self, clave: str, cantidad: int = 1):
        with self.lock:
            if clave in self.contadores:
                self.contadores[clave] += cantidad

    def registrar_latencia(self, latencia_ms: float):
        if latencia_ms >= 0:
            with self.lock:
                self.latencias_ms.append(latencia_ms)

    def resumen(self, extra: dict = None) -> dict:
        with self.lock:
            lats = sorted(self.latencias_ms)
            p95 = round(lats[int(len(lats) * 0.95)], 2) if lats else 0.0
            res = {"counters": dict(self.contadores), "latency_p95_ms": p95}
            if extra:
                res.update(extra)
            return res