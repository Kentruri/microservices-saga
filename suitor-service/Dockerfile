FROM python:3.10-slim
WORKDIR /app

COPY suitor-service/requirements.txt .
COPY suitor-service/app.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]