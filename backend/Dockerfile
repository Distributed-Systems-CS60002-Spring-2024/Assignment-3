FROM python:3.10.12-slim

WORKDIR /app

COPY ./app /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt

CMD ["python", "server.py"]

EXPOSE 5000