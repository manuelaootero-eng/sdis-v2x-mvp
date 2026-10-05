FROM python:3.10-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar boto3
RUN pip install --no-cache-dir boto3

# Copiar los scripts al contenedor
COPY consumidor_v2x.py .
COPY productor_v2x.py .

# Comando por defecto al arrancar el contenedor
CMD ["python3", "consumidor_v2x.py"]