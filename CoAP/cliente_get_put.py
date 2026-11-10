import asyncio
import aiocoap
import sys
import warnings

async def main():
    contexto = await aiocoap.Context.create_client_context()

    print("1. Haciendo petición GET a /temperatura...")
    mensaje_get = aiocoap.Message(code=aiocoap.GET, uri="coap://127.0.0.1/temperatura")
    respuesta_get = await contexto.request(mensaje_get).response
    print(f"[RECIBIDO] {respuesta_get.payload.decode('utf-8')}\n")

    print("2. Haciendo petición PUT a /calefaccion enviando orden 'APAGAR'...")
    # PUT es un mensaje Confirmable (CON) por defecto
    mensaje_put = aiocoap.Message(code=aiocoap.PUT, uri="coap://127.0.0.1/calefaccion", payload=b"APAGAR")
    respuesta_put = await contexto.request(mensaje_put).response
    print(f"[RECIBIDO] Confirmación: {respuesta_put.payload.decode('utf-8')}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())