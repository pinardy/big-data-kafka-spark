FROM python:3.9-alpine

WORKDIR /consumer

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-u", "./consumer.py"]
