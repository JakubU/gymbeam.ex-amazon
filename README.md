# Amazon Data Extractor

## Description

This component facilitates the extraction of:

- **FBA** inventory snapshots (daily summaries)
- **FBA Ledger** detail and summary reports
- **FBM** orders, returns, and financial events
- **Amazon Ads** campaign reports (Sponsored Products, Brands, Display)

It integrates with Keboola Connection (KBC) to automate API data retrieval and loading into tables.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Features](#features)
- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
- [Execution Flags](#execution-flags)
- [Output](#output)
- [Development](#development)
- [Integration](#integration)

## Prerequisites

- **Amazon Seller Account** with SP-API & Ads API access
- **API Credentials** registered via LWA:
  - `refresh_token`, `app_id`, `client_secret_id`
  - `refresh_token_ads`, `app_id_ads`, `client_secret_id_ads`

## Features

| Feature                     | Description                                                         |
| --------------------------- | ------------------------------------------------------------------- |
| FBA Inventory Snapshots     | Daily full snapshots of FBA inventory with pagination               |
| FBA Ledger Detail & Summary | Bank-statement style inventory movements (detail + aggregated views) |
| FBM Orders                  | Incremental orders by last update                                   |
| FBM Returns                 | Incremental returns extraction (XML)                                |
| FBM Financial Events        | Paged retrieval of financial transactions                           |
| Amazon Ads Reports          | Daily campaign reports for Sponsored Products/Brands/Display        |
| Execution Flags             | Toggle each extraction step                                          |
| Multi-marketplace Support   | Configure multiple FBA marketplaces                                 |
| Robust Error Handling       | Rate-limit backoff & detailed logging                               |

## Supported Endpoints

- **FBA** `/fba/inventory/v1/summaries`
- **Ledger Detail** `GET_LEDGER_DETAIL_VIEW_DATA`
- **Ledger Summary** `GET_LEDGER_SUMMARY_VIEW_DATA`
- **Orders** `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`
- **Returns** `GET_XML_RETURNS_DATA_BY_RETURN_DATE`
- **Finances** `/finances/v0/financialEvents`
- **Ads Reporting** `POST /reporting/reports` & `GET /reporting/reports/{reportId}`

## Configuration

Set parameters in `config.json` or the KBC UI:

```json
{
  "marketplace_id": "<FBM Marketplace ID>",
  "#refresh_token": "<Seller refresh token>",
  "#app_id": "<LWA App ID>",
  "#client_secret_id": "<LWA Client Secret>",
  "date_range": 7,

  "#refresh_token_ads": "<Ads refresh token>",
  "#app_id_ads": "<Ads App ID>",
  "#client_secret_id_ads": "<Ads Client Secret>",
  "stores": [
    { "name": "Amazon.it", "scope": "..." },
    { "name": "Amazon.de", "scope": "..." }
  ],

  "inventory_fba": {
    "marketplace_ids": ["A1PA6795UKMFR9", "APJ6JRA9NG5V4"]
  },

  "execution": {
    "run_inventory": true,
    "run_orders": true,
    "run_returns": true,
    "run_finances": true,
    "run_ads": true
  }
}
```

## Execution Flags

- `run_inventory` – FBA inventory snapshots
- `run_orders`    – FBM orders
- `run_returns`   – FBM returns
- `run_finances`  – FBM financial events
- `run_ads`       – Amazon Ads reports

## Output

| Table Name                         | Description                          |
| ---------------------------------- | ------------------------------------ |
| `inventory.csv`                    | FBA inventory snapshots              |
| `inventory_ledger_detail.csv`      | FBA ledger detail view               |
| `inventory_ledger_summary.csv`     | FBA ledger summary view              |
| `orders.csv`                       | FBM orders                           |
| `returns.csv`                      | FBM returns                          |
| `finance.csv`                      | FBM financial events                 |
| `advertising.csv`                  | Amazon Ads campaign reports          |

## Development

To set up locally:

```bash
git clone https://github.com/JakubU/gymbeam.ex-amazon.git
cd gymbeam.ex-amazon
docker-compose build
docker-compose run --rm dev
```

## Integration

Use KBC orchestration to schedule this extractor alongside transformations and writers for end-to-end ETL pipelines.
