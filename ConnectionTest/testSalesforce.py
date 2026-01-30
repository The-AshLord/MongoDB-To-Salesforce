import os
import logging
from simple_salesforce import Salesforce
from dotenv import load_dotenv

load_dotenv()

# Configuración de logs
logging.basicConfig(
    filename='conexion_salesforce.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def conectar_salesforce():
    try:
        sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_TOKEN"),
            domain=os.getenv("SF_DOMAIN"),
            client_id='Salvant ETL'
        )
        logging.info("Conexión exitosa a Salesforce")
        return sf
    except Exception as e:
        logging.error(f"Error de conexión: {e}")
        raise
def main():
    sf = conectar_salesforce()
if __name__ == "__main__":
    main()