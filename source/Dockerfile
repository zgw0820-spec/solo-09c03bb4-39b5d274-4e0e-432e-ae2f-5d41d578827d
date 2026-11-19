FROM python:3.11-slim
WORKDIR /app
COPY source/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY source/app ./app
EXPOSE 123/udp 9000
CMD ["sh", "-c", "uvicorn app.server:app --host 0.0.0.0 --port ${HTTP_PORT:-9000}"]
