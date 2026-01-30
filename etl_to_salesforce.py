import os
import logging
from pymongo import MongoClient
from datetime import datetime
from simple_salesforce import Salesforce
from dotenv import load_dotenv

load_dotenv() 

# ---------- CONFIG LOGS ----------
logging.basicConfig(
    filename="etl_salesforce.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- CONEXIÓN MONGO ----------
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "OrdenesTest"
COLLECTION_NAME = "Ordenes"
def conectar_mongo():
    logging.info("Conectado a MongoDB")
    return MongoClient(MONGO_URI)
    

# ---------- CONEXIÓN SALESFORCE ----------
def conectar_salesforce():
    try:
        sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_TOKEN"),
            domain=os.getenv("SF_DOMAIN"),
            client_id=os.getenv("CLIENT")
        )
        logging.info("Conectado a Salesforce")
        return sf
    except Exception as e:
        logging.error(f"Error Salesforce: {e}")
        raise

# ---------- TRANSFORMACIONES ----------
def map_stage(status):
    mapping = {
        "new": "Prospecting",
        "process": "Qualification",
        "sent": "Proposal/Price Quote",
        "finished": "Closed Won",
        "returned": "Closed Lost"
    }
    return mapping.get((status or "").lower(), "Prospecting")

def format_date(date_value):
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    try:
        return datetime.strptime(str(date_value), "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return datetime.today().strftime("%Y-%m-%d")

def safe_float(value):
    try:
        return float(value)
    except:
        return 0.0

# ---------- ETL ----------
def transformar_orden(doc):
    if not isinstance(doc, dict):
        return None

    edi = doc.get("edi") or {}
    customer = doc.get("customer") or "SinCliente"
    shipment = str(doc.get("shipmentid") or "SinID")

    name_union = f"{customer} - {shipment}"

    return {
        "External_Id__c": str(doc.get("shipmentid", "")),
        "TrackingNumber__c": str(doc.get("shipment_id", "")),
        "Name": name_union, #doc.get("customer") or "Sin Cliente",
        "CloseDate": format_date(doc.get("date")),
        "Amount": safe_float(edi.get("flat_rate")),
        "StageName": map_stage(doc.get("status")),
        "Description": doc.get("description") or ""
    }

def extraer_y_transformar():
    client = conectar_mongo()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    oportunidades = []

    try:
        for doc in collection.find({}):
            opp = transformar_orden(doc)
            if opp and opp["External_Id__c"]:
                oportunidades.append(opp)

    except Exception as e:
        logging.error(f"Error ETL: {e}")

    finally:
        client.close()
        logging.info("Mongo cerrado")

    return oportunidades

# ---------- UPSERT ----------
def upsert_opportunity(sf, opp):
    try:
        ext_id = opp["External_Id__c"]
        body = opp.copy()
        body.pop("External_Id__c", None)

        sf.Opportunity.upsert(
            f"External_Id__c/{ext_id}",
            body
        )

        logging.info(f"Upsert OK {ext_id}")

    except Exception as e:
        logging.error(f"Error Upsert {ext_id}: {e}")

# ---------- MAIN ----------
def main():
    sf = conectar_salesforce()
    oportunidades = extraer_y_transformar()

    logging.info(f"Total registros: {len(oportunidades)}")

    for opp in oportunidades:
        upsert_opportunity(sf, opp)

    logging.info("Proceso finalizado")

if __name__ == "__main__":
    main()
