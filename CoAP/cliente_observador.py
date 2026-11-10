import asyncio
import aiocoap
import sys
import warnings

async def main():
    contexto = await aiocoap.Context.create_client_context()

    print("[*] Suscribiéndose a los cambios de /temperatura (Modo Observe)...\n")
    # Hacemos el GET, pero añadiendo la opción Observe
    mensaje = aiocoap.Message(code=aiocoap.GET, uri="coap://127.0.0.1/temperatura", observe=0)
    solicitud = contexto.request(mensaje)
    
    try:
        # 1. Esperamos el estado actual inicial
        respuesta = await solicitud.response
        print(f"Estado Inicial: {respuesta.payload.decode('utf-8')}")
        
        # 2. Nos quedamos en bucle infinito escuchando las notificaciones automáticas (push)
        async for respuesta_asincrona in solicitud.observation:
            print(f"Notificación (Push del servidor): {respuesta_asincrona.payload.decode('utf-8')}")
            
    except Exception as e:
        print(f"[ERROR] Se perdió la observación: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Cliente observador apagado.")