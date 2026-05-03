# star-schema-warehouse

Enterprise-grade Telco/SaaS data warehouse using **dbt + Snowflake**.

## Architecture

| Layer | Pattern | Snowflake Schema | Materialisation |
|-------|---------|-----------------|----------------|
| Staging | Bronze — clean & typed 1:1 with sources | `STG` | View |
| Raw Vault | Data Vault 2.0 — immutable hubs/links/sats | `RAW_VAULT` | Incremental (merge) |
| Business Vault | Derived vault entities (CLV, MRR, churn) | `BUS_VAULT` | Table |
| Dimensions | Kimball SCD1/SCD2 dims | `DIM` | Table |
| Facts | Event-grain & periodic snapshot facts | `FCT` | Incremental (merge) |
| Marts | Gold domain-oriented aggregates + OBT | `MART` | Table |

## Project Layout

```
models/
  staging/          # Bronze: clean + typed
  raw_vault/        # Data Vault 2.0 (hubs, links, satellites)
  business_vault/   # CLV, MRR, churn computations
  dimensions/       # Kimball dims (SCD1 & SCD2)
  facts/            # Subscription, event, invoice fact tables
  marts/            # Revenue, churn, usage, One Big Table
snapshots/          # SCD2 source snapshots
macros/             # hash_key, generate_surrogate_key, etc.
tests/              # Custom data quality assertions
seeds/              # Reference lookup tables
analyses/           # Ad-hoc analyses (cohort retention)
```

## Quick Start

### Prerequisites
- Python 3.11+
- dbt-snowflake >= 1.8.0
- Snowflake account with `TRANSFORMER` role

### Setup

```bash
# Install dbt
pip install dbt-snowflake>=1.8.0 elementary-data>=0.15.0 dbt-checkpoint

# Copy and populate profiles
cp profiles.yml.example profiles.yml
# Edit profiles.yml or export env vars:
# SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

# Install dbt packages
dbt deps

# Run seeds (reference data)
dbt seed

# Build all models
dbt build

# Generate & serve docs
dbt docs generate && dbt docs serve
```

### Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## Data Sources

| Source | Description |
|--------|-------------|
| `raw.customers` | CRM customer master |
| `raw.subscriptions` | Subscription lifecycle |
| `raw.events` | Product usage events |
| `raw.invoices` | Billing transactions |
| `raw.products` | Product/plan catalog |

## Key Metrics

- **MRR / ARR**: Monthly & Annual Recurring Revenue
- **Churn Rate**: Customer churn as % of total
- **NRR**: Net Revenue Retention
- **GRR**: Gross Revenue Retention
- **CLV**: Customer Lifetime Value
- **DAU/MAU**: Daily/Monthly Active Users

## CI/CD

- **CI** (`ci.yml`): Runs on every PR — `dbt build --select state:modified+` + Elementary alerts
- **Deploy** (`deploy.yml`): Runs on merge to `main` — full `dbt build` + docs publish

## Data Vault 2.0

Hubs carry the business key + load metadata. Links record relationships.
Satellites store all descriptive attributes with full history via `load_date` / `load_end_date` + `hashdiff`.

Hash keys are SHA-256 based via the `hash_key()` macro.

## SCD Type 2

Source-level snapshots (`snapshots/`) use dbt's `snapshot` materialisation with `strategy: check`.
Dimension tables join the snapshot output to expose `valid_from` / `valid_to` columns for Kimball-style SCD2 dims.
