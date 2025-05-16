# Amazon Data Extractor

## Description

This component facilitates the extraction of FBA inventory snapshots, FBM orders, returns, financial events, and Amazon Advertising reports from the Amazon Selling Partner and Advertising APIs. It is designed to be deployed within the Keboola Connection (KBC) ecosystem, leveraging its processing capabilities to handle API data efficiently.

## Table of Contents

- [Functionality Notes](#functionality-notes)
- [Prerequisites](#prerequisites)
- [Features](#features)
- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
- [Execution Flags](#execution-flags)
- [Output](#output)
- [Development](#development)
- [Integration](#integration)

## Functionality Notes

This component is developed to manage data flows from Amazon’s APIs into KBC, supporting both FBM and FBA use cases. It handles pagination, incremental loading, and offers toggles for each extraction step, ensuring efficient and customizable data pipelines.

## Prerequisites

- **Amazon Seller Account**: Access to Amazon Seller Central and Advertising Console.
- **API Credentials**: Register your application with Amazon to obtain:
  - `refresh_token`
  - `app_id`
  - `client_secret_id`
  - `refresh_token_ads`
  - `app_id_ads`
  - `client_secret_id_ads`

## Features

| **Feature**              | **Description**                                                |
|--------------------------|----------------------------------------------------------------|
| FBA Inventory Snapshots  | Daily full snapshots of inventory with pagination support      |
| FBM Orders               | Incremental orders extraction using customizable date ranges   |
| FBM Returns              | Incremental returns extraction with XML detail                 |
| FBM Financial Events     | Paged retrieval of financial transactions and adjustments     |
| Amazon Ads Reports       | Sponsored Products, Brands, and Display campaign reports      |
| Execution Flags          | Toggle each extraction step on or off                         |
| Multiple Marketplaces    | Configure multiple FBA marketplaces and a single FBM endpoint |
| Robust Error Handling    | Retries with exponential backoff and detailed logging         |

## Supported Endpoints

- **FBA Inventory**: `/fba/inventory/v1/summaries`
- **All Orders (Flat File)**: `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`
- **Returns (XML)**: `GET_XML_RETURNS_DATA_BY_RETURN_DATE`
- **Financial Events**: `/finances/v0/financialEvents`
- **Amazon Ads Reporting**: `POST /reporting/reports` and `GET /reporting/reports/{reportId}`

For additional endpoint support, submit a request at [ideas.keboola.com](https://ideas.keboola.com/).

## Configuration

Define parameters in `config.json` or via the KBC UI under **Parameters**:

```json
{
  "marketplace_id": "<FBM Marketplace ID>",
  "#refresh_token": "<Seller Central refresh token>",
  "#app_id": "<LWA App ID>",
  "#client_secret_id": "<LWA Client Secret>",
  "date_range": "<days back for FBM orders/returns/finances>",

  "#refresh_token_ads": "<Ads API refresh token>",
  "#app_id_ads": "<Ads LWA App ID>",
  "#client_secret_id_ads": "<Ads LWA Client Secret>",
  "stores": [
    { "name": "Amazon.it", "scope": "<Ads Scope>" },
    { "name": "Amazon.de", "scope": "<Ads Scope>" }
  ],

  "inventory_fba": {
    "marketplace_ids": ["A1PA6795UKMFR9", "ATVPDKIKX0DER"]
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

### Amazon Ads Configuration

- **stores**: A list of store objects:
  ```json
  "stores": [
    { "name": "Amazon.it", "scope": "665807000098197" },
    { "name": "Amazon.de", "scope": "2780716582721957" }
  ]
  ```

## Execution Flags

Toggle individual extraction steps under the **execution** section:

- `run_inventory`: Execute FBA inventory snapshot
- `run_orders`: Execute FBM orders extraction
- `run_returns`: Execute FBM returns extraction
- `run_finances`: Execute FBM financial events extraction
- `run_ads`: Execute Amazon Ads report extraction

## Output

The component produces the following tables in KBC:

- `inventory.csv` (FBA inventory snapshots)
- `orders.csv` (FBM orders)
- `returns.csv` (FBM returns)
- `finance.csv` (FBM financial events)
- `advertising.csv` (Amazon Ads reports)

## Development

Set up your development environment by cloning the repository and running the component:

```bash
git clone https://github.com/JakubU/gymbeam.ex-amazon.git
cd gymbeam.ex-amazon
docker-compose build
docker-compose run --rm dev
```

## Integration

This component can be integrated within bigger ETL workflows in KBC. Use KBC orchestration to schedule and chain this extractor with transformations, writers, and other components for end-to-end data pipelines.
