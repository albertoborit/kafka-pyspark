# Descargar la version 15 de Node
FROM node:15

# Usar el directorio kafka-server
WORKDIR /kafka-server

# Copiar todos package.json al directorio
COPY kafka-server/package*.json ./

# Instalar dependencias del package.json
RUN npm install

# Copiar el archivo del servidor al directorio
COPY kafka-server/app.js .

# Exponer el puerto 3000
EXPOSE 3000

# Inicializar el servidor
CMD ["node", "app.js"]
