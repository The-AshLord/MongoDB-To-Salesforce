import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("MONGO_URI")

client = MongoClient(uri)

try:
    db = client["OrdenesTest"]
    collection = db["Ordenes"]

    datos = list(collection.find({}))
    print(datos)

except Exception as e:
    print("Error de conexión:", e)

finally:
    client.close()
    print("Conexión cerrada")