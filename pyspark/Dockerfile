# Descarga de la ultima version imagen de pyspark
FROM jupyter/pyspark-notebook:latest

# Asignacion de la carpeta app
WORKDIR /app

# Uso el usuario con privilegios
USER root

# Actualizacion los paquetes e instalamos Java JRE 8
RUN apt-get update && \
    apt-get install -y openjdk-8-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Asignacion el usuario NB_UID
USER $NB_UID

# Instalacion de requests, pyspark y faker como dependencias
RUN pip install requests pyspark faker

# Copia del script
COPY pyspark/script.py .

# Ejecucion del script
CMD ["python", "script.py"]
