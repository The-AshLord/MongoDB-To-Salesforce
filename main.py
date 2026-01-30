import os
import logging
from pymongo import MongoClient
from datetime import datetime
from simple_salesforce import Salesforce
from dotenv import load_dotenv

load_dotenv() 

# ---------- CONFIG LOGS ----------
logging.basicConfig(
    filename="mongo_a_salesforce.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ------------ MONGODB ------------
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "OrdenesTest"
COLLECTION_NAME = "Ordenes"
def mongo_connection():
    logging.info("Conectado a MongoDB")
    return MongoClient(MONGO_URI)
    
# ----------- SALESFORCE ----------
def salesforce_connection():
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

# -------- DATA TRANSFORMATION --------
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
def order_conversion(doc):
    if not isinstance(doc, dict):
        return None

    edi = doc.get("edi") or {}
    customer = doc.get("customer") or "Without a client"
    shipment = str(doc.get("shipmentid") or "Without ID")

    name_union = f"{customer} - {shipment}"

    return {
        "External_Id__c": str(doc.get("shipmentid", "")),
        "TrackingNumber__c": str(doc.get("shipment_id", "")),
        "Name": name_union, 
        "CloseDate": format_date(doc.get("date")),
        "Amount": safe_float(edi.get("flat_rate")),
        "Advances__c": safe_float(edi.get("advances")),
        "StageName": map_stage(doc.get("status")),
        "OrderNumber__c": str(doc.get("load_number", "")),
        "Office__c": str(doc.get("office", "")),
        "Order_Type__c":str(doc.get("order_type", "")),
        "Weight__c":str(edi.get("weight", "")),
        "Description": doc.get("description") or ""
    }

def extraction():
    client = mongo_connection()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    opportunities = []

    try:
        for doc in collection.find({}):
            opp = order_conversion(doc)
            if opp and opp["External_Id__c"]:
                opportunities.append(opp)

    except Exception as e:
        logging.error(f"Error ETL: {e}")

    finally:
        client.close()
        logging.info("MongoDB cerrado")

    return opportunities

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

def main():
    sf = salesforce_connection()
    opportunities = extraction()

    logging.info(f"Total registros: {len(opportunities)}")

    for opp in opportunities:
        upsert_opportunity(sf, opp)

    logging.info("Proceso finalizado")

if __name__ == "__main__":
    main()
