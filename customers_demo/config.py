"""Configuration for the customers_demo lakehouse project."""

CATALOG_NAME = "customers_demo"
SCHEMAS = ["bronze", "silver", "framework"]

S3_BUCKET = "s3://customers-demo-data/"
STORAGE_CREDENTIAL = "assurant-s3-credential"
EXTERNAL_LOCATION_NAME = "customers_s3_location"
PERMISSION_EXT_LOCATION = "framework_s3_ext_loc"
AUTHORIZED_USERS = [
    "srinivas.akana@capgemini.com",
    "mirza-shoeb.baig@capgemini.com",
    "akshay.chaudhary@capgemini.com",
]

BRONZE_TABLES = {
    "agent_event_details": "s3://customers-demo-data/raw/Agnt_Evnt_Detl_Stage/",
    "agents": "s3://customers-demo-data/raw/Agnt_Stage/",
    "call_types": "s3://customers-demo-data/raw/CallTyp_Stage/",
    "claim_payment": "s3://customers-demo-data/raw/Claim_Payment_Stage/",
    "coverage": "s3://customers-demo-data/raw/Coverage_Stage/",
    "fact_claim": "s3://customers-demo-data/raw/Fact_Claim_Stage/",
    "fact_service_request": "s3://customers-demo-data/raw/Fact_Service_req_Stage/",
    "party": "s3://customers-demo-data/raw/Party_Stage/",
    "policy": "s3://customers-demo-data/raw/Plcy_Stage/",
    "product": "s3://customers-demo-data/raw/Prdct_Stage/",
    "route_call_details": "s3://customers-demo-data/raw/Rout_Call_Detl_stage/",
    "service_request": "s3://customers-demo-data/raw/Service_req_Stage/",
    "terminate_calls": "s3://customers-demo-data/raw/Terminate_call_stage/",
}

OBSOLETE_BRONZE_TABLES = ["customers_demo.bronze.customers"]
OBSOLETE_SILVER_TABLES = ["customers_demo.silver.customers"]

SILVER_TABLES = [
    (f"{CATALOG_NAME}.silver.{table_name}", table_name)
    for table_name in BRONZE_TABLES
]
