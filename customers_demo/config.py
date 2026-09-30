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
    "customers": "s3://customers-demo-data/raw/Custmr_Stg/",
    "agents": "s3://customers-demo-data/raw/Agnt/",
    "agent_event_details": "s3://customers-demo-data/raw/Agnt_Evnt_Detl_Stage/",
    "call_types": "s3://customers-demo-data/raw/CallTyp_Stage/",
    "route_call_details": "s3://customers-demo-data/raw/Rout_Call_Detl_stage/",
    "terminate_calls": "s3://customers-demo-data/raw/Terminate_call_stage/",
}

DQ_RULE = "Drop rows with NULL in business columns unless the source is temporarily exempted"
DQ_NULL_EXEMPT_TABLES = {"customers_demo.bronze.terminate_calls"}

DQ_TABLES = [
    (
        "customers_demo.bronze.agent_event_details",
        "customers_demo.silver.agent_event_details",
        ["RKey", "Date_Time", "STID", "DomID", "ReCode", "Duration"],
    ),
    (
        "customers_demo.bronze.agents",
        "customers_demo.silver.agents",
        ["TID", "PID", "EntName"],
    ),
    (
        "customers_demo.bronze.call_types",
        "customers_demo.silver.call_types",
        ["CTID", "EName", "Description", "SLT"],
    ),
    (
        "customers_demo.bronze.customers",
        "customers_demo.silver.customers",
        ["first_name", "last_name", "email", "gender", "Mobile", "Address", "State"],
    ),
    (
        "customers_demo.bronze.route_call_details",
        "customers_demo.silver.route_call_details",
        [
            "RecoveryKey",
            "DateTime",
            "RouterCallKey",
            "RouterCallKeyDay",
            "MRDomainID",
            "CallTypeID",
            "ScriptID",
            "SkillGroupSkillTargetID",
            "Label",
            "RouteDispositionCode",
        ],
    ),
    (
        "customers_demo.bronze.terminate_calls",
        "customers_demo.silver.terminate_calls",
        [
            "RKey",
            "DateTime",
            "RCalKey",
            "RouterCallKeyDay",
            "MRDomID",
            "PeriprealD",
            "SGroupSTID",
            "AgntSkllTrgtID",
            "CTID",
            "CallDisposition",
            "CallDispositionFlag",
            "Duration",
            "RingTime",
            "DelayTime",
            "HoldTime",
            "TalkTime",
            "WorkTime",
            "LocalQTime",
            "AnsWaitTime",
            "AnsweredWithinSL",
            "CTCategoryID",
            "NetworkTime",
        ],
    ),
]

SILVER_TABLES = [
    ("customers_demo.silver.agent_event_details", "agent_event_details"),
    ("customers_demo.silver.agents", "agents"),
    ("customers_demo.silver.call_types", "call_types"),
    ("customers_demo.silver.customers", "customers"),
    ("customers_demo.silver.route_call_details", "route_call_details"),
    ("customers_demo.silver.terminate_calls", "terminate_calls"),
]
