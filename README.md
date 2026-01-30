# MongoDB To Salesforce
Technical challenge for Savant: a small automation in Python to perform an ETL *(Extract, Transform, Load)* process that migrates order data from a **MongoDB Atlas cluster** into **Salesforce Developer Edition** as **Opportunities**. Each record from the **Ordenes** collection is transformed and upserted into Salesforce while preventing duplicates through an External ID field.

## Requirements
### Software:
- Python 3.10+
- Salesforce Developer Edition account
- MongoDB Cluster Connection String
- IDE (Visual Studio Code is recommended)

### Python Libraries:
- **pymongo** – Connect and query MongoDB
- **simple-salesforce** – Interact with Salesforce REST/SOAP APIs
- **python-dotenv** – Manage environment variables securely
- **logging (built-in)** – Error tracking and execution logs

## Installation
### Step 1: Set up your Python Environment

- Install [Python](https://www.python.org)
-  Set up your Virtual Environment:
  
in Windows CMD/PowerShell
```
python -m venv venv
venv\Scripts\activate
```

**Install Required Packages**

```
pip install simple-salesforce pymongo python-dotenv
```
### Step 2: Securely Store Your Credentials
Create a `.env` File
To avoid hard-coding your Salesforce credentials into the script, we’ll use environment variables. This keeps sensitive data out of your source code and version control.
In your project folder, create a file called .env with the following contents:
```
MONGO_URI=mongodb+srv://<user>:<password>@cluster0.mongodb.net/
SF_USERNAME=your_email@example.com
SF_PASSWORD=your_password
SF_TOKEN=your_security_token_from_Salesforce
SF_DOMAIN=login
```
### Step 3: Salesforce Configuration

- Create a new Developer Edition Org.
- Enable **API Access** in your user profile.
- Eneable **SOAP API login()** in Setup -> Traslation Workbench ->  User Interface
- Create a custom field in Opportunity:
  - **Field Name:** External_Id__c
  - **Type:** Text
  - **External ID:** Enabled
  - **Unique:** Enabled
### Step 4: MongoDB Configuration
- Add your public IP address in MongoDB Atlas → Network Access → IP Access List.
- Ensure the database contains the Ordenes collection.

## Run the program
```
python main.py
```
