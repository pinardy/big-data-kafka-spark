FROM docker.io/bitnami/spark:3.5.5

WORKDIR /scripts
COPY requirements.txt .

USER root
RUN pip3 install -r requirements.txt
RUN pip install pyspark pandas numpy mlflow boto3 psycopg2-binary


RUN apt-get update && apt-get install -y curl
#
#ENV SPARK_JARS_IVY=/tmp/.ivy2/jars
#RUN mkdir -p /tmp/.ivy2/jars && chmod -R 777 /tmp/.ivy2

RUN curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
  && mv postgresql*jar /opt/bitnami/spark/jars/


EXPOSE 7077 8080
