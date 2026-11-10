# Demo DDS · Eclipse Cyclone DDS + Python

Sigue la guía en PDF (Windows 10/11, macOS y Ubuntu). Resumen:

| Fichero | Para qué |
|---|---|
| setup.ps1 / setup.sh | Instalación automática (Windows / macOS y Ubuntu) |
| check_install.py | Comprueba Python y Cyclone DDS |
| sensor_types.py | Tipo de datos del topic |
| qos_profiles.py | Perfiles QoS: basic, durable, deadline |
| publisher.py | Sensor de temperatura (DataWriter) |
| subscriber.py | Analítica Edge (DataReader + alarmas QoS) |
| dds_net.py, cyclonedds.xml.example | Configuración de red opcional |

Requisito: Python 3.10 – 3.13 (recomendado 3.12).
