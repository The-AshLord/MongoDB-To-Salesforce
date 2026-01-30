import os
import logging
from datetime import datetime
from pymongo import MongoClient
from simple_salesforce import Salesforce
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG LOGS ----------
logging.basicConfig(
    filename="account_salesforce.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# CONEXIONES
# -----------------------

DB_NAME = "OrdenesTest"
COLLECTION_NAME = "Ordenes"
def conectar_mongo():
    client = MongoClient(os.getenv("MONGO_URI"))
    logging.info("Conectado a MongoDB")
    return client

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

# -----------------------
# STATUS MAP
# -----------------------

def map_status(status):
    status = (status or "").lower()

    mapping = {
        "new": {"stage": "Prospecting", "delivery": "Pending"},
        "process": {"stage": "Qualification", "delivery": "In Progress"},
        "sent": {"stage": "Proposal/Price Quote", "delivery": "Scheduled"},
        "finished": {"stage": "Closed Won", "delivery": "Completed"},
        "returned": {"stage": "Closed Lost", "delivery": "Cancelled"}
    }

    return mapping.get(status, {
        "stage": "Prospecting",
        "delivery": "Yet to begin"
    })
    

# -----------------------
# ACCOUNT UPSERT
# -----------------------

def upsert_account(sf, order):

    STATE_MAP = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "IL": "Illinois",
        "IN": "Indiana",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "MA": "Massachusetts",
        "MD": "Maryland",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MO": "Missouri",
        "NC": "North Carolina",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NV": "Nevada",
        "NY": "New York",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "SC": "South Carolina",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VA": "Virginia",
        "WA": "Washington",
        "WI": "Wisconsin"
    }
    customer = order.get("customer", "Unknown")

    edi = order.get("edi")
    if not isinstance(edi, dict):
        edi = {}

    external_id = customer.strip()

    raw_state = edi.get("stop1_st")
    mapped_state = STATE_MAP.get(raw_state)

    account_data = {
        "Name": customer,

        "BillingCountry": "United States"
    }

    # Solo agregar estado si existe en el mapa
    if mapped_state:
        account_data["BillingState"] = mapped_state

    try:
        logging.info(f"Upsert Account {customer}")

        result = sf.Account.upsert(
            f"External_Id__c/{external_id}",
            account_data
        )

        if result.get("created"):
            logging.info(f"Account creada {customer}")
            return result.get("id")

        query = sf.query(
            f"SELECT Id FROM Account WHERE External_Id__c = '{external_id}' LIMIT 1"
        )

        acc_id = query["records"][0]["Id"]
        logging.info(f"Account existente {customer} ID {acc_id}")
        return acc_id

    except Exception as e:
        logging.error(f"Error Account {customer}: {e}")
        return None
# -----------------------
# TRANSFORM OPPORTUNITY
# -----------------------

def transformar_oportunidad(doc, account_id):
    logging.info(f"Transformando orden {doc.get('shipmentid')}")

    status_map = map_status(doc.get("status"))

    shipment = doc.get("shipmentid", "")
    customer = doc.get("customer", "")
    name = f"{customer} - {shipment}"

    close_date = doc.get("date")
    if close_date:
        close_date = datetime.strptime(close_date, "%Y-%m-%d").date()
    else:
        close_date = datetime.today().date()

    opp = {
        "External_Id__c": shipment,
        "Name": name,
        "TrackingNumber__c": str(doc.get("shipment_id", "")),
        "StageName": status_map["stage"],
        "DeliveryInstallationStatus__c": status_map["delivery"],
        "Amount": float(doc.get("flat_rate")),
        "CloseDate": str(close_date),
        "OrderNumber__c": str(doc.get("load_number", "")),
        "Description": doc.get("description"),
        "AccountId": account_id
    }

    logging.info(f"Oportunidad transformada: {name}")
    return opp

# -----------------------
# UPSERT OPPORTUNITY
# -----------------------

def upsert_opportunity(sf, opp):

    external_id = opp["External_Id__c"]

    try:
        sf.Opportunity.upsert(
            f"External_Id__c/{external_id}",
            opp
        )
        logging.info(f"Upsert OK {external_id}")

    except Exception as e:
        logging.error(f"Error Upsert {external_id}: {e}")

# -----------------------
# MAIN
# -----------------------

def main():

    mongo = conectar_mongo()
    sf = conectar_salesforce()

    db = mongo["OrdenesTest"]
    ordenes = db["Ordenes"]

    for doc in ordenes.find():

        # 1. ACCOUNT
        account_id = upsert_account(sf, doc)
        if not account_id:
            continue

        # 2. OPPORTUNITY
        opp = transformar_oportunidad(doc, account_id)
        upsert_opportunity(sf, opp)

    mongo.close()
    logging.info("Proceso Finalizado")


if __name__ == "__main__":
    main()