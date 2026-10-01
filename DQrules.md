# Data Quality Rules and Data Model

This document records the supplied insurance and interaction schemas, key relationships, and engineering data-quality controls. It is a design/reference document; the rules are not automatically enforced by the current bronze-to-silver pipeline.

## Engineering Data-Quality Rules

| Rule ID | Level | Control / Rule | Severity | Disposition | Evidence / Notification |
|---|---|---|---|---|---|
| ENG_ING_002 | Schema | Incoming schema matches registry | Critical | Fail batch or approved additive drift | Schema repository + alert |
| ENG_DQ_R01 | Record | Mandatory field / null check | Critical/High | Reject or quarantine | DQ log |
| ENG_DQ_R02 | Record | Datatype validation | Critical | Reject or quarantine | DQ log |
| ENG_DQ_R05 | Record | Duplicate detection by configured key | High | Deduplicate or quarantine | DQ log by key |
| ENG_DQ_REF01 | Referential | Party_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF02 | Referential | Policy_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF03 | Referential | Policy_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF04 | Referential | Product_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF05 | Referential | Coverage_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF06 | Referential | Call_type_sk_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF07 | Referential | Service_request_sk_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF08 | Referential | Service_fact_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF09 | Referential | Claims_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF10 | Referential | Payment_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF11 | Referential | Agent_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF12 | Referential | Agent_event_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF13 | Referential | Route_call_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_DQ_REF14 | Referential | Termination_sk exists and is unique | High | Reject or flag | DQ log |
| ENG_ING_004 | Reconciliation | Rows read = written + rejected | Critical | Fail reconciliation | DQ log |
| ENG_ING_005 | Reconciliation | Duplicate rows | Critical | Duplicate reconciliation | DQ log |
| ENG_ING_006 | Reconciliation | Null rows | Critical | Null reconciliation | DQ log |
| ENG_DQ_MED01 | Medallion | Bronze to Silver record reconciliation | Critical | Fail reconciliation | Pipeline audit + layer counts |
| ENG_DQ_CAN01 | Canonical | Source records conform to canonical Interaction, Party and Intent model | Critical | Quarantine nonconforming records | Canonical validation log |

## Table Schemas

### PARTY

| Column | Datatype | Key / Relationship |
|---|---|---|
| `party_sk` | BIGINT | PK |
| `enterprise_customer_id` | VARCHAR(100) | |
| `party_bk` | VARCHAR(100) | |
| `party_name` | VARCHAR(255) | |
| `first_name` | VARCHAR(100) | |
| `last_name` | VARCHAR(100) | |
| `date_of_birth` | DATE | |
| `email_address` | VARCHAR(255) | |
| `phone_number` | VARCHAR(50) | |
| `party_status_cd` | VARCHAR(20) | |
| `party_type_cd` | VARCHAR(20) | |
| `preferred_language_cd` | VARCHAR(10) | |
| `effective_dtm` | TIMESTAMP | |
| `expiry_dtm` | TIMESTAMP | |
| `current_ind` | CHAR(1) | |
| `created_dtm` | TIMESTAMP | |
| `updated_dtm` | TIMESTAMP | |

### POLICY

| Column | Datatype | Key / Relationship |
|---|---|---|
| `policy_sk` | BIGINT | PK |
| `enterprise_policy_id` | VARCHAR(100) | |
| `policy_bk` | VARCHAR(100) | |
| `party_sk` | BIGINT | FK to `PARTY.party_sk` |
| `product_sk` | BIGINT | FK to `PRODUCT.product_sk` |
| `property_sk` | BIGINT | FK to `PROPERTY.property_sk` |
| `policy_number` | VARCHAR(100) | |
| `policy_type_cd` | VARCHAR(50) | |
| `policy_status_cd` | VARCHAR(20) | |
| `policy_start_dt` | DATE | |
| `policy_end_dt` | DATE | |
| `annual_premium_amt` | DECIMAL(18,2) | |
| `effective_dtm` | TIMESTAMP | |
| `expiry_dtm` | TIMESTAMP | |
| `current_ind` | CHAR(1) | |

### PRODUCT

| Column | Datatype | Key / Relationship |
|---|---|---|
| `product_sk` | BIGINT | PK |
| `enterprise_product_id` | VARCHAR(100) | |
| `product_bk` | VARCHAR(100) | |
| `product_code` | VARCHAR(50) | |
| `product_name` | VARCHAR(255) | |
| `product_category_cd` | VARCHAR(50) | |
| `lob_cd` | VARCHAR(50) | |
| `active_ind` | CHAR(1) | |
| `effective_dt` | DATE | |

### PROPERTY

The supplied schema lists `property_sk`, `property_type_cd`, `city_nm`, and `state_cd`; datatypes and a complete PK declaration were not included. The `property_sk` is referenced as an FK by `POLICY`.

### COVERAGE

| Column | Datatype | Key / Relationship |
|---|---|---|
| `coverage_sk` | BIGINT | PK |
| `policy_sk` | BIGINT | FK to `POLICY.policy_sk` |
| `coverage_code` | VARCHAR(50) | |
| `coverage_name` | VARCHAR(255) | |
| `coverage_type_cd` | VARCHAR(50) | |
| `coverage_limit_amt` | DECIMAL(18,2) | |
| `deductible_amt` | DECIMAL(18,2) | |
| `effective_dt` | DATE | |
| `expiration_dt` | DATE | |

### CALL_TYPE

| Column | Datatype | Key / Relationship |
|---|---|---|
| `call_type_sk` | INT | PK |
| `call_type_cd` | VARCHAR(32) | |
| `call_type_name` | VARCHAR(255) | |
| `call_category_cd` | INT | |
| `active_ind` | INT | |

### SERVICE_REQUEST

| Column | Datatype | Key / Relationship |
|---|---|---|
| `service_request_sk` | BIGINT | PK |
| `party_sk` | BIGINT | FK to `PARTY.party_sk` |
| `policy_sk` | BIGINT | FK to `POLICY.policy_sk` |
| `request_number` | VARCHAR(100) | |
| `request_type_cd` | VARCHAR(50) | |
| `request_status_cd` | VARCHAR(20) | |
| `opened_dtm` | TIMESTAMP | |
| `closed_dtm` | TIMESTAMP | |
| `resolution_hrs` | INTEGER | |

### FACT_SERVICE_REQUEST

| Column | Datatype | Key / Relationship |
|---|---|---|
| `service_fact_sk` | BIGINT | PK |
| `date_sk` | BIGINT | FK to `DATE.date_sk` |
| `party_sk` | BIGINT | FK to `PARTY.party_sk` |
| `policy_sk` | BIGINT | FK to `POLICY.policy_sk` |
| `service_request_sk` | BIGINT | FK to `SERVICE_REQUEST.service_request_sk` |
| `resolution_hrs` | INTEGER | Measure |
| `request_count` | INTEGER | Measure |

### FACT_CLAIM

| Column | Datatype | Key / Relationship |
|---|---|---|
| `claim_sk` | BIGINT | PK |
| `enterprise_claim_id` | VARCHAR(100) | |
| `policy_sk` | BIGINT | FK to `POLICY.policy_sk` |
| `party_sk` | BIGINT | FK to `PARTY.party_sk` |
| `claim_number` | VARCHAR(100) | |
| `claim_type_cd` | VARCHAR(50) | |
| `claim_status_cd` | VARCHAR(20) | |
| `loss_dt` | DATE | |
| `reported_dt` | DATE | |
| `closed_dt` | DATE | |
| `claimed_amt` | DECIMAL(18,2) | Measure |
| `approved_amt` | DECIMAL(18,2) | Measure |
| `settlement_amt` | DECIMAL(18,2) | Measure |
| `fraud_ind` | CHAR(1) | |

### CLAIM_PAYMENT

| Column | Datatype | Key / Relationship |
|---|---|---|
| `payment_sk` | BIGINT | PK |
| `claim_sk` | BIGINT | FK to `FACT_CLAIM.claim_sk` |
| `payment_number` | VARCHAR(100) | |
| `payment_amt` | DECIMAL(18,2) | Measure |
| `payment_dt` | DATE | |
| `payment_status_cd` | VARCHAR(20) | |
| `payment_type_cd` | VARCHAR(20) | |

### AGENT

| Column | Datatype | Key / Relationship |
|---|---|---|
| `agent_sk` | BIGINT | PK |
| `agent_id` | STRING(100) | |
| `agent_name` | STRING(255) | |
| `team_name` | STRING(100) | |
| `manager_name` | STRING(255) | |
| `agent_status_cd` | STRING(20) | |
| `region_cd` | STRING(50) | |

### ROUTE_CALL_DETAIL

| Column | Datatype | Key / Relationship |
|---|---|---|
| `route_call_sk` | BIGINT | PK |
| `contact_id` | VARCHAR(100) | |
| `customer_sk` | BIGINT | Relationship not specified; confirm whether this is `party_sk` |
| `call_type_sk` | BIGINT | Candidate FK to `CALL_TYPE.call_type_sk`; confirm |
| `routing_queue_cd` | VARCHAR(100) | |
| `routing_profile_cd` | VARCHAR(100) | |
| `channel_cd` | VARCHAR(20) | |
| `call_start_dtm` | TIMESTAMP | |
| `call_end_dtm` | TIMESTAMP | |
| `total_duration_sec` | INTEGER | Measure |
| `transfer_ind` | CHAR(1) | |

### TERMINATION_CALL_DETAIL

| Column | Datatype | Key / Relationship |
|---|---|---|
| `termination_call_sk` | BIGINT | PK |
| `route_call_sk` | BIGINT | Candidate FK to `ROUTE_CALL_DETAIL.route_call_sk`; confirm |
| `contact_id` | STRING | |
| `termination_reason_cd` | STRING | |
| `disconnect_party_cd` | STRING | |
| `call_outcome_cd` | STRING | |
| `termination_dtm` | TIMESTAMP | |

### AGENT_EVENT_DETAIL

| Column | Datatype | Key / Relationship |
|---|---|---|
| `agent_event_sk` | BIGINT | PK |
| `contact_id` | STRING | |
| `agent_sk` | BIGINT | Candidate FK to `AGENT.agent_sk`; confirm |
| `route_call_sk` | BIGINT | Candidate FK to `ROUTE_CALL_DETAIL.route_call_sk`; confirm |
| `event_type_cd` | STRING | |
| `event_start_dtm` | TIMESTAMP | |
| `event_end_dtm` | TIMESTAMP | |
| `event_duration_sec` | INT | Measure |

### DATE

This dimension was specified earlier and is referenced by `FACT_SERVICE_REQUEST`.

| Column | Datatype | Key / Relationship |
|---|---|---|
| `date_sk` | BIGINT | PK |
| `calendar_dt` | DATE | |
| `month_nm` | STRING | |
| `quarter_nbr` | INT | |
| `year_nbr` | INT | |

## Foreign-Key Relationships

| Child table | Child column | Parent table | Parent column | Status |
|---|---|---|---|---|
| POLICY | `party_sk` | PARTY | `party_sk` | Supplied |
| POLICY | `product_sk` | PRODUCT | `product_sk` | Supplied |
| POLICY | `property_sk` | PROPERTY | `property_sk` | Supplied |
| COVERAGE | `policy_sk` | POLICY | `policy_sk` | Supplied |
| SERVICE_REQUEST | `party_sk` | PARTY | `party_sk` | Supplied |
| SERVICE_REQUEST | `policy_sk` | POLICY | `policy_sk` | Supplied |
| FACT_SERVICE_REQUEST | `date_sk` | DATE | `date_sk` | Supplied |
| FACT_SERVICE_REQUEST | `party_sk` | PARTY | `party_sk` | Supplied |
| FACT_SERVICE_REQUEST | `policy_sk` | POLICY | `policy_sk` | Supplied |
| FACT_SERVICE_REQUEST | `service_request_sk` | SERVICE_REQUEST | `service_request_sk` | Supplied |
| FACT_CLAIM | `policy_sk` | POLICY | `policy_sk` | Supplied |
| FACT_CLAIM | `party_sk` | PARTY | `party_sk` | Supplied |
| CLAIM_PAYMENT | `claim_sk` | FACT_CLAIM | `claim_sk` | Inferred from supplied claim schema; confirm |
| ROUTE_CALL_DETAIL | `customer_sk` | PARTY | `party_sk` | Confirm key/name alignment |
| ROUTE_CALL_DETAIL | `call_type_sk` | CALL_TYPE | `call_type_sk` | Confirm |
| TERMINATION_CALL_DETAIL | `route_call_sk` | ROUTE_CALL_DETAIL | `route_call_sk` | Confirm |
| AGENT_EVENT_DETAIL | `agent_sk` | AGENT | `agent_sk` | Confirm |
| AGENT_EVENT_DETAIL | `route_call_sk` | ROUTE_CALL_DETAIL | `route_call_sk` | Confirm |

## Neo4j Relationship Cardinalities

| Parent Entity | Child Entity | Cardinality | Comment |
|---|---|---|---|
| PARTY | POLICY | 1:M | One party can own multiple policies |
| PRODUCT | POLICY | 1:M | One product can be associated to many policies |
| POLICY | COVERAGE | 1:M | One policy contains multiple coverages |
| PARTY | SERVICE_REQUEST | 1:M | One party can raise multiple requests |
| SERVICE_REQUEST | FACT_SERVICE_REQUEST | 1:M | One request may generate multiple events |
| POLICY | FACT_CLAIM | 1:M | One policy can generate many claims |
| FACT_CLAIM | CLAIM_PAYMENT | 1:M | One claim can have multiple payments |
| PARTY | ROUTE_CALL_DETAIL | 1:M | One party can make multiple contacts |
| CALL_TYPE | ROUTE_CALL_DETAIL | 1:M | One call type can classify many calls |
| ROUTE_CALL_DETAIL | AGENT_EVENT_DETAIL | 1:M | One call can have many agent events |
| AGENT | AGENT_EVENT_DETAIL | 1:M | One agent performs many events |
| ROUTE_CALL_DETAIL | TERMINATION_CALL_DETAIL | 1:1 | One routed call has one termination outcome |

## Items to Confirm Before Enforcing Rules

- `ENG_DQ_REF03` duplicates the `Policy_sk` check in `ENG_DQ_REF02`; confirm the intended key/table.
- `Call_type_sk_sk` and `Service_request_sk_sk` appear to contain duplicated `_sk` suffixes.
- `Claims_sk` and `Termination_sk` do not exactly match the declared `claim_sk` and `termination_call_sk` columns.
- `ROUTE_CALL_DETAIL.customer_sk` should be confirmed against the renamed `PARTY.party_sk`.
- The FK relationships marked candidate/inferred must be confirmed against the actual source-table schemas before building Neo4j relationships or rejecting records.
- Bronze-to-silver currently copies data as-is. The Neo4j stage creates these relationships and validates the declared 1:1 route-call/termination cardinality.
