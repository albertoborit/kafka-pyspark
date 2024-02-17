# Usamos la imagen de Java 11 JRE
FROM openjdk:11-jre-slim

# Seteamos el directorio
WORKDIR /app

# Copiamos el instalador de kafka y las configuraciones
# del servidor
COPY ./kafka/kafka_2.13-2.8.0.tgz /app/
COPY ./kafka/server.properties /app/kafka/config

# Extraemos Kafka
RUN tar -xzf kafka_2.13-2.8.0.tgz && \
    mv kafka_2.13-2.8.0 /kafka

# Seteamos variables de entorno
ENV KAFKA_HOME=/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

# Creamos el directorio de data de kafka
RUN mkdir /kafka/data

# Exponemos los puertos de Kafka and Zookeeper
EXPOSE 9092 2181

# Inicializamos el servidor Kafka
CMD ["sh", "-c", "/kafka/bin/kafka-server-start.sh /kafka/config/server.properties"]
