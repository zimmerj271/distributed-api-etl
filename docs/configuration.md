# Configuration Guide

This document describes how to configure pipelines in the Spark API-Driven ETL Framework.

## Configuration Overview

A pipeline configuration is composed of:

* **tables** – source & sink definitions
* **endpoint** – HTTP request definition
* **auth** – authentication strategy
* **transport** – HTTP engine selection
* **middleware** – retries, logging, auth injection
* **execution** – partitioning and rate control

All configs are **strictly validated** via Pydantic after preprocessing.

## Full Configuration Template

```yaml
execution:
  partitions: 200
  max_requests_per_second: 30

transport:
  base_timeout: 30
  warmup_timeout: 10
  tcp_connection:
    limit: 10

auth:
  type: oauth2_password
  token_url: "https://auth.example.com/token"
  client_id: "{{secret.DATABRICKS_SCOPE:CLIENT_ID_KEY}}"
  client_secret: "{{secret.DATABRICKS_SCOPE:CLIENT_SECRET_KEY}}"
  username: "{{secret.DATABRICKS_SCOPE:USERNAME_KEY}}"
  password: "{{secret.DATABRICKS_SCOPE:PASSWORD_KEY}}"

endpoint:
  method: POST
  base_path: "https://api.example.com/v1"
  url_path: "https://api.example.com/v1/patient"
  headers:
    Accept: "application/json"
    Content-Type: "application/json"

middleware:
  - type: retry
    max_attempts: 3
    retry_status_codes: [429, 500, 502, 503]
    base_delay: 0.2
    max_delay: 2.0
  - type: logging
  - type: timing
  - type: json_body

tables:
  source:
    name: api_staging
    namespace: catalog.schema
    id_column: tracking_id

    required_columns:
      - tracking_id
      - patient_id

  sink:
    name: api_response
    namespace: catalog.schema
    mode: append

  column_mapping:
    - source_column: patient_id
      endpoint_param: patient
```

## Configuration Sections

### Execution

Controls parallelism and rate limiting.

| Parameter | Type | Description |
|-----------|------|-------------|
| `partitions` | int | Number of Spark partitions for parallel execution |
| `max_requests_per_second` | int | Rate limit per partition |

### Transport

Configures the HTTP engine.

| Parameter | Type | Description |
|-----------|------|-------------|
| `base_timeout` | int | Default request timeout in seconds |
| `warmup_timeout` | int | Timeout for connection warm-up |
| `tcp_connection.limit` | int | Maximum concurrent TCP connections |

### Auth

Configures authentication strategy. Supported types:

| Type | Description |
|------|-------------|
| `none` | No authentication |
| `basic` | HTTP Basic Auth (username/password) |
| `bearer` | Static bearer token |
| `oauth2_password` | OAuth2 Resource Owner Password Credentials |
| `oauth2_client_credentials` | OAuth2 Client Credentials |

### Endpoint

Defines the HTTP request template.

| Parameter | Type | Description |
|-----------|------|-------------|
| `method` | string | HTTP method (GET, POST, PUT, DELETE) |
| `base_path` | string | Base URL for the API |
| `url_path` | string | Full URL path for requests |
| `headers` | dict | HTTP headers to include |

### Middleware

List of middleware to apply to each request. See [Middleware Documentation](middleware.md) for details.

### Tables

Defines source and sink table configurations.

#### Source Table

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Table name |
| `namespace` | string | Catalog.schema namespace |
| `id_column` | string | Column containing unique request IDs |
| `required_columns` | list | Columns that must exist in source |

#### Sink Table

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Table name |
| `namespace` | string | Catalog.schema namespace |
| `mode` | string | Write mode (append, overwrite) |

#### Column Mapping

Maps source columns to endpoint parameters:

```yaml
column_mapping:
  - source_column: patient_id
    endpoint_param: patient
  - source_column: visit_date
    endpoint_param: date
```

## Secrets Handling

Secrets are resolved **before** Pydantic validation using template syntax:

```yaml
# Databricks secrets
password: "{{secret.SCOPE_NAME:SECRET_KEY}}"

# Environment variables
api_key: "{{env.API_KEY}}"
```

### Databricks Secrets

Format: `{{secret.SCOPE:KEY}}`

Resolved via `dbutils.secrets.get(scope="SCOPE", key="KEY")`

### Environment Variables

Format: `{{env.VARIABLE_NAME}}`

Resolved via `os.environ["VARIABLE_NAME"]`

**Important:** Runtime components never access environment variables or secret stores directly. All secrets are resolved during configuration preprocessing on the driver.

## Example Configurations

### No Authentication

```yaml
auth:
  type: none

endpoint:
  method: GET
  url_path: "https://api.example.com/public/data"
```

### Basic Authentication

```yaml
auth:
  type: basic
  username: "{{env.API_USERNAME}}"
  password: "{{env.API_PASSWORD}}"
```

### OAuth2 Client Credentials

```yaml
auth:
  type: oauth2_client_credentials
  token_url: "https://auth.example.com/oauth/token"
  client_id: "{{secret.MY_SCOPE:CLIENT_ID}}"
  client_secret: "{{secret.MY_SCOPE:CLIENT_SECRET}}"
  scope: "read write"
```

## Validation

All configurations are validated using Pydantic models. Invalid configurations fail fast with descriptive error messages before any Spark jobs are submitted.

Common validation errors:

* Missing required fields
* Invalid authentication type
* Malformed URLs
* Unknown middleware types
* Type mismatches (e.g., string where int expected)
