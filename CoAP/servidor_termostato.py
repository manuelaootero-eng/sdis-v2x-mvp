import asyncio
import aiocoap
import aiocoap.resource as resource
import sys
import warnings

class Temperatura(resource.ObservableResource):
    def __init__(self):
        super().__init__()
        self.temp = 20.0
        # Tarea en segundo plano para subir la temperatura
        asyncio.create_task(self.simular_calor())

    async def simular_calor(self):
        """Sube la temperatura cada 3 segundos y notifica a los observadores."""
        while True:
            await asyncio.sleep(3)
            self.temp += 0.5
            print(f"[SERVIDOR] Temperatura subió a {self.temp:.1f}ºC. Notificando...")
            self.updated_state() # Empuja los datos a los clientes suscritos

    async def render_get(self, request):
        """Responde a las peticiones GET devolviendo la temperatura actual."""
        payload = f"Temperatura: {self.temp:.1f} ºC".encode('utf-8')
        return aiocoap.Message(payload=payload)

class Calefaccion(resource.Resource):
    def __init__(self):
        super().__init__()
        self.estado = "ENCENDIDA"

    async def render_put(self, request):
        """Responde a peticiones PUT para cambiar el estado de la calefacción."""
        comando = request.payload.decode('utf-8')
        print(f"\n[SERVIDOR] ¡Orden recibida en /calefaccion! Nuevo estado: {comando}\n")
        self.estado = comando
        
        # Devolvemos un código de éxito (2.04 Changed)
        payload = f"Calefacción {self.estado}".encode('utf-8')
        return aiocoap.Message(code=aiocoap.CHANGED, payload=payload)

async def main():
    root = resource.Site()
    root.add_resource(['temperatura'], Temperatura())
    root.add_resource(['calefaccion'], Calefaccion())
    
    print("[INFO] Servidor CoAP Termostato iniciado en coap://127.0.0.1:5683")
    await aiocoap.Context.create_server_context(root, bind=("127.0.0.1", 5683))
    await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    if sys.platform == 'win32':
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Servidor apagado.")