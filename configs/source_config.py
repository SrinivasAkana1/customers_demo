"""Source and bronze table configuration."""

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
    "customers": "s3://customers-demo-data/raw/Custmr_Stg/",
    "agents": "s3://customers-demo-data/raw/Agnt/",
    "agent_event_details": "s3://customers-demo-data/raw/Agnt_Evnt_Detl_Stage/",
    "call_types": "s3://customers-demo-data/raw/CallTyp_Stage/",
    "route_call_details": "s3://customers-demo-data/raw/Rout_Call_Detl_stage/",
    "terminate_calls": "s3://customers-demo-data/raw/Terminate_call_stage/",
}

BRONZE_TABLES_LIST = [
    {"s3_file": "agents.csv", "table_name": "agents"},
    {"s3_file": "customer_calls.csv", "table_name": "customer_calls"},
    {"s3_file": "customers.csv", "table_name": "customers"},
    {"s3_file": "ivr_call_details.csv", "table_name": "ivr_call_details"},
    {"s3_file": "route_call_details.csv", "table_name": "route_call_details"},
    {"s3_file": "terminate_calls.csv", "table_name": "terminate_calls"},
]
