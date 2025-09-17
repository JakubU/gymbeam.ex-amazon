# Amazon Data Extractor

## Description

This comprehensive component facilitates the extraction of Amazon e-commerce data through both the Selling Partner API (SP-API) and Amazon Advertising API:

- **FBA** inventory snapshots and planning data
- **FBA Ledger** detail and summary reports  
- **FBM** orders, returns, and financial events
- **Amazon Ads** campaign reports (Sponsored Products, Brands, Display)
- **Strategic Products** sales rankings and performance data

It integrates with Keboola Connection (KBC) to automate API data retrieval and loading into structured tables.

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
  - `refresh_token`, `app_id`, `client_secret_id` (Seller Central)
  - `refresh_token_ads`, `app_id_ads`, `client_secret_id_ads` (Amazon Ads)
- **Input Table** with `products_asin` column (for strategic products analysis)
  - Required only when `run_startegic_products` is enabled
  - Must contain at least one column named `products_asin` with valid Amazon ASINs
  - Example: CSV with columns `product_id`, `products_asin`, `product_name`

## Features

| Feature                     | Description                                                         |
| --------------------------- | ------------------------------------------------------------------- |
| FBA Inventory Snapshots     | Daily full snapshots of FBA inventory with pagination               |
| FBA Inventory Planning      | Strategic planning data for inventory optimization                  |
| FBA Ledger Detail & Summary | Bank-statement style inventory movements (detail + aggregated views) |
| FBM Orders                  | Incremental orders by last update across multiple marketplaces      |
| FBM Returns                 | Incremental returns extraction (XML)                                |
| FBM Financial Events        | Paged retrieval of financial transactions                           |
| Amazon Ads Reports          | Daily campaign reports for Sponsored Products/Brands/Display        |
| Strategic Products Analysis | Sales rankings and performance data for specific ASINs              |
| Execution Flags             | Toggle each extraction step                                          |
| Multi-marketplace Support   | Configure multiple Amazon marketplaces simultaneously               |
| Robust Error Handling       | Rate-limit backoff & detailed logging                               |

## Supported Endpoints

- **FBA Inventory** `/fba/inventory/v1/summaries`
- **FBA Planning** `GET_FBA_INVENTORY_PLANNING_DATA`
- **Ledger Detail** `GET_LEDGER_DETAIL_VIEW_DATA`
- **Ledger Summary** `GET_LEDGER_SUMMARY_VIEW_DATA`
- **Orders** `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`
- **Returns** `GET_XML_RETURNS_DATA_BY_RETURN_DATE`
- **Finances** `/finances/v0/financialEvents`
- **Strategic Products** `/catalog/2022-04-01/items`
- **Ads Reporting** `POST /reporting/reports` & `GET /reporting/reports/{reportId}`

## Configuration

Set parameters in `config.json` or the KBC UI:

```json
{
  "#refresh_token": "<Seller refresh token>",
  "#app_id": "<LWA App ID>",
  "#client_secret_id": "<LWA Client Secret>",
  "#refresh_token_ads": "<Ads refresh token>",
  "#app_id_ads": "<Ads App ID>",
  "#client_secret_id_ads": "<Ads Client Secret>",
  "date_range": 7,
  
  "stores": [
    { "name": "Amazon.it", "scope": "1234567890" },
    { "name": "Amazon.de", "scope": "0987654321" }
  ],
  
  "marketplaces": [
    {
      "marketplace_id": "APJ6JRA9NG5V4"
    },
    {
      "marketplace_id": "A1PA6795UKMFR9"
    }
  ],
  
  "execution": {
    "run_inventory": true,
    "run_inventory_planning": true,
    "run_orders": true,
    "run_returns": true,
    "run_finances": true,
    "run_ads": true,
    "run_ledger": true,
    "run_startegic_products": true
  }
}
```

## Execution Flags

- `run_inventory` – FBA inventory snapshots
- `run_inventory_planning` – FBA inventory planning data
- `run_orders`    – FBM orders
- `run_returns`   – FBM returns
- `run_finances`  – FBM financial events
- `run_ads`       – Amazon Ads reports
- `run_ledger`    – FBA ledger reports (detail and summary)
- `run_startegic_products` – Strategic products sales rankings

## Output

| Table Name                         | Description                          |
| ---------------------------------- | ------------------------------------ |
| `inventory.csv`                    | FBA inventory snapshots              |
| `inventory_planning.csv`           | FBA inventory planning data          |
| `inventory_ledger_detail.csv`      | FBA ledger detail view               |
| `inventory_ledger_summary.csv`     | FBA ledger summary view              |
| `orders.csv`                       | FBM orders                           |
| `returns.csv`                      | FBM returns                          |
| `finance.csv`                      | FBM financial events                 |
| `advertising.csv`                  | Amazon Ads campaign reports          |
| `amazon_strategic_products_rank.csv` | Strategic products sales rankings |

## Development

To set up locally:

```bash
git clone https://github.com/JakubU/gymbeam.ex-amazon.git
cd gymbeam.ex-amazon
docker-compose build
docker-compose run --rm dev
```

## Integration

Use KBC orchestration to schedule this extractor alongside transformations and writers for end-to-end ETL pipelines. The component supports both manual and automated triggers for data synchronization.
