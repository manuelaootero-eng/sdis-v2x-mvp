FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumidor_v2x.py .
COPY productor_v2x.py .

EXPOSE 80
EXPOSE 1883

CMD ["python3", "-u", "consumidor_v2x.py"]