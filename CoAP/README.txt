TALLER PRÁCTICO CoAP - TERMOSTATO INTELIGENTE
=============================================

PREPARACIÓN DEL ENTORNO
-----------------------
Abre una terminal en la carpeta del proyecto y ejecuta:

1. Crear un entorno virtual:
   python -m venv venv

2. Activar el entorno virtual:
   - En Windows: .\venv\Scripts\activate
   - En Ubuntu:  source venv/bin/activate

3. Instalar la librería requerida:
   pip install aiocoap


EJECUCIÓN DE LA PRÁCTICA
------------------------
Para probar todos los scripts, abre 3 terminales distintas en tu editor/terminal (con el entorno virtual activo en todos ellos).

TERMINAL 1: Iniciar el Servidor (Termostato)
Ejecuta:
python servidor_termostato.py
(Déjalo corriendo en segundo plano. Pulsa Ctrl+C para salir)

TERMINAL 2: Probar operaciones REST clásicas (GET y PUT)
Ejecuta:
python cliente_get_put.py

TERMINAL 3: Probar el modo asíncrono (Observe)
Ejecuta:
python cliente_observador.py
(Verás cómo el servidor envía datos automáticamente cada 3 segundos. Pulsa Ctrl+C para salir).