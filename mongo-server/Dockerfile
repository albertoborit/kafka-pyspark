# Usamos la ultima version e la imagen de mongoDB
FROM mongo:latest

# Seteamos el directorio
WORKDIR /usr/src/app

# Copiamos la configuracion de la base de datos
COPY mongo-server/mongod.conf /etc/mongod.conf

# Exponemos el puerto de MongoDB
EXPOSE 27017

# Inicializamos la instancia
CMD ["mongod", "--config", "/etc/mongod.conf"]
