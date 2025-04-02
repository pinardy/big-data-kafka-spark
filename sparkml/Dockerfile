FROM bitnami/spark:3.1.2

WORKDIR /scripts
COPY requirements.txt .

USER root
RUN pip3 install -r requirements.txt
USER 1001
