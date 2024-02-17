from pyspark.sql import SparkSession
import requests
import json
import faker

# Creacion de sesion en Spark
spark = SparkSession.builder.appName("API") \
    .config("spark.log.level", "DEBUG") \
    .getOrCreate()

# Definicion del endpoint de Node
api_url = "http://nodejs-server:3000/api/sendToKafka"

# Numero de solicitudes al servidor
num_requests = 10000

# Asignacion del creador de data aleatoria
fake = faker.Faker()

def create_fake_user_data():
    user_data = {
        "user": fake.name(),
        "service": fake.word(),
        "data": fake.text()
    }
    return user_data

# Generacion de data aleatoria
fake_user_data = [create_fake_user_data() for _ in range(num_requests)]

# Envio de solicitudes al servidor de forma secuencial
responses = []
for i, user_data in enumerate(fake_user_data):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=user_data, headers=headers)
    print("response", response)
    response.raise_for_status()
    responses.append(response.json())

# Mostrar respuestas
for i, response in enumerate(responses):
    if "error" in response:
        print(response["error"])
    else:
        print(f"Response {i + 1}: {response}")

# Detener script de spark
spark.stop()
