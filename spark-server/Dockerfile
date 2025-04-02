FROM bitnami/spark:3.1.2

WORKDIR /scripts
COPY requirements.txt .

USER root
RUN pip3 install -r requirements.txt
USER 1001


RUN curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
  && mv postgresql*jar /tmp/


EXPOSE 7077 8080
