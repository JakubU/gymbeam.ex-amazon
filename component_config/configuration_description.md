# Configuration Description for Amazon Extractor

## Description

This component is designed to connect to Amazon's Selling Partner API and Amazon Ads API to extract comprehensive e-commerce data including FBA inventory, FBM orders, returns, financial events, and advertising reports. It's optimized for use within the Keboola Connection (KBC) ecosystem and supports multiple Amazon marketplaces.

## Configuration Sections

### Storage

#### Input
- **Tables**: Input table containing ASINs for strategic products analysis (required for strategic products extraction)
  - **Required Column**: `products_asin` - Contains Amazon Standard Identification Numbers (ASINs) to analyze
  - **Usage**: The component reads this table to extract unique ASINs and fetch their sales rankings across configured marketplaces
  - **Format**: CSV file with at least one column named `products_asin`
  - **Example**: Table with columns like `product_id`, `products_asin`, `product_name` where `products_asin` contains values like "B08N5WRWNW", "B07H8QMZWV"

#### Output
- **Tables**: Multiple output tables containing extracted data:
  - `inventory.csv` - FBA inventory snapshots
  - `inventory_planning.csv` - FBA inventory planning data
  - `inventory_ledger_detail.csv` - FBA ledger detail view
  - `inventory_ledger_summary.csv` - FBA ledger summary view
  - `orders.csv` - FBM orders
  - `returns.csv` - FBM returns
  - `finance.csv` - FBM financial events
  - `advertising.csv` - Amazon Ads campaign reports
  - `amazon_strategic_products_rank.csv` - Strategic products sales rankings

### Parameters

#### Authentication
- **#refresh_token**, **#app_id**, **#client_secret_id**: OAuth credentials for Amazon Seller Central API access
- **#refresh_token_ads**, **#app_id_ads**, **#client_secret_id_ads**: OAuth credentials for Amazon Ads API access

#### Data Extraction
- **date_range**: Number of days to look back for data extraction (default: 7)
- **stores**: Array of Amazon stores with their Advertising API scopes for ads reporting
- **marketplaces**: Array of Amazon marketplaces with optional strategic products configuration

#### Execution Control
- **execution**: Object containing boolean flags to control which extraction steps to run:
  - `run_inventory` - FBA inventory snapshots
  - `run_inventory_planning` - FBA inventory planning data
  - `run_orders` - FBM orders
  - `run_returns` - FBM returns
  - `run_finances` - FBM financial events
  - `run_ads` - Amazon Ads reports
  - `run_ledger` - FBA ledger reports (detail and summary)
  - `run_startegic_products` - Strategic products sales rankings

### Obtaining API Credentials and Marketplace ID

1. **Marketplace ID**: Identify the Amazon marketplace ID relevant to your geographic sales region. This ID is crucial as it determines which Amazon data you can access. [See Marketplace IDs](https://developer-docs.amazon.com/sp-api/docs/marketplace-ids).

2. **Register for Amazon API Access**:
    - Sign in to your Amazon Seller Central account.
    - Navigate to the 'Apps & Services' section and select 'Develop Apps'.
    - Follow the process to create a new application, which will provide you with the necessary `app_id` and other details.
    - [Connecting to the Selling Partner API Guide](https://developer-docs.amazon.com/sp-api/docs/connecting-to-the-selling-partner-api?ld=NSGoogle) provides a step-by-step approach.

3. **Authentication Credentials**:
    - Once your application is created, you will receive a `refresh_token`, `app_id`, and `client_secret_id`.
    - These credentials will be used to authenticate API requests made by this component.

### Authorization

Describes OAuth credentials setup for secure access:
- **Credentials**: Contains the OAuth credentials authorized by the user, including refresh tokens and client secrets. 
- **oauthVersion**: Indicates the OAuth version supported by the component, which is "2.0" for Amazon API.

## Functionality

### Data Handling
- **Incremental Loading**: The component supports incremental data loading, which means only new or updated records are fetched in subsequent runs based on the specified date range.
- **Error Handling**: Implements robust error handling to manage API rate limits and possible disconnections or API errors.

### Development and Customization
- Developers can clone and set up the component for customization. They can build upon existing functionalities or add support for additional endpoints as per user or business requirements.

### API Endpoints
- The component handles data from multiple Amazon API endpoints related to orders, returns,financial transactions and inventory FBA. Each endpoint is selected based on the configuration parameters set by the user in KBC.

## Integration

The component is fully integrated into the KBC platform, allowing users to configure and schedule data extraction jobs directly from their KBC projects. It supports both manual and automated triggers for data synchronization.

### FBA Inventory Configuration

This section configures daily extraction of FBA inventory snapshots across one or more Amazon Marketplaces using the SP‑API `getInventorySummaries` endpoint (details: https://developer-docs.amazon.com/sp-api/reference/getinventorysummaries).

- **marketplaces** _(array[object], required)_: List of Amazon Marketplace objects containing marketplace_id and optional strategic_products
- **details**: Always set to `true` to retrieve detailed inventory fields
- **pagination**: Uses `nextToken` in response to fetch subsequent pages until exhausted

**Output Table**: `inventory.csv` includes the following key columns:

| Column                       | Source Path                                                   | Description                                                     |
|------------------------------|---------------------------------------------------------------|-----------------------------------------------------------------|
| `asin`                       | `inventorySummaries[].asin`                                   | Amazon Standard Identification Number                           |
| `fn_sku`                     | `inventorySummaries[].fnSku`                                  | Fulfillment network SKU (FNSKU)                                 |
| `seller_sku`                 | `inventorySummaries[].sellerSku`                              | Seller-defined SKU                                              |
| `condition`                  | `inventorySummaries[].condition`                              | Condition of the item (e.g., NewItem)                           |
| `last_updated_time`          | `inventorySummaries[].lastUpdatedTime`                        | Timestamp of last update                                        |
| `product_name`               | `inventorySummaries[].productName`                            | Title or name of the product                                    |
| `total_quantity`             | `inventorySummaries[].totalQuantity`                          | Sum of all quantities (sellable + unsellable + in-transit)      |
| `stores`                     | `inventorySummaries[].stores`                                 | Array of store codes where stock is available                   |
| `marketplace_id`             | n/a                                                           | Marketplace ID used for this snapshot                           |
| `extracted_at`               | n/a                                                           | ISO timestamp when extraction was performed                     |
| `fulfillable_quantity`       | `inventoryDetails.fulfillableQuantity`                        | Quantity ready for sale                                         |
| `inbound_working_quantity`   | `inventoryDetails.inboundWorkingQuantity`                     | Quantity currently being processed in inbound workflow          |
| `inbound_shipped_quantity`   | `inventoryDetails.inboundShippedQuantity`                     | Quantity already shipped to FC, awaiting receiving confirmation |
| `inbound_receiving_quantity` | `inventoryDetails.inboundReceivingQuantity`                   | Quantity in transit to FC                                       |
| `total_reserved_quantity`    | `inventoryDetails.reservedQuantity.totalReservedQuantity`     | Quantity reserved for existing orders                           |
| `pending_customer_order_quantity` | `inventoryDetails.reservedQuantity.pendingCustomerOrderQuantity` | Reserved for pending customer orders              |
| `pending_transshipment_quantity`  | `inventoryDetails.reservedQuantity.pendingTransshipmentQuantity` | Reserved for cross-border transfers          |
| `fc_processing_quantity`     | `inventoryDetails.reservedQuantity.fcProcessingQuantity`      | FC-internal processing reservations                             |
| `total_researching_quantity` | `inventoryDetails.researchingQuantity.totalResearchingQuantity` | Quantity flagged for research or QA review           |
| `researching_quantity_breakdown` | `inventoryDetails.researchingQuantity.researchingQuantityBreakdown` | Breakdown of researching quantities (list of name/quantity pairs) |
| `total_unfulfillable_quantity` | `inventoryDetails.unfulfillableQuantity.totalUnfulfillableQuantity` | Quantity deemed unsellable (all reasons)     |
| `customer_damaged_quantity`  | `inventoryDetails.unfulfillableQuantity.customerDamagedQuantity` | Unsellable due to customer damage      |
| `warehouse_damaged_quantity` | `inventoryDetails.unfulfillableQuantity.warehouseDamagedQuantity` | Unsellable due to warehouse damage      |
| `distributor_damaged_quantity` | `inventoryDetails.unfulfillableQuantity.distributorDamagedQuantity` | Unsellable due to distributor damage   |
| `carrier_damaged_quantity`   | `inventoryDetails.unfulfillableQuantity.carrierDamagedQuantity` | Unsellable due to carrier damage           |
| `defective_quantity`         | `inventoryDetails.unfulfillableQuantity.defectiveQuantity`    | Unsellable due to defects                                       |
| `expired_quantity`           | `inventoryDetails.unfulfillableQuantity.expiredQuantity`      | Unsellable due to expiration                                     |

This approach ensures column names are ≤ 64 characters and remain descriptive. For full API schema, see: https://developer-docs.amazon.com/sp-api/reference/getinventorysummaries

### FBA Inventory Planning Configuration

This section configures extraction of FBA Inventory Planning data using the `GET_FBA_INVENTORY_PLANNING_DATA` report type.

**Output Table**: `inventory_planning.csv` contains planning data with columns including:
- `snapshot-date` - Date of the planning snapshot
- `sku` - Seller SKU
- `asin` - Amazon Standard Identification Number
- `marketplace_id` - Marketplace ID
- `extracted_at` - Extraction timestamp

### Strategic Products Configuration

This section configures extraction of sales rankings for strategic products using the Catalog Items API.

**Requirements**:
- **Input Table**: Required table with `products_asin` column containing ASINs to analyze
  - Must contain at least one column named `products_asin`
  - ASINs should be valid Amazon Standard Identification Numbers
  - Component processes up to 20 ASINs per API call for optimal performance
- **Marketplace Configuration**: `marketplaces` array with marketplace IDs where rankings should be fetched

**Input Table Format**:
```csv
product_id,products_asin,product_name
1,B08N5WRWNW,Product A
2,B07H8QMZWV,Product B
3,B09XYZ1234,Product C
```

**Output Table**: `amazon_strategic_products_rank.csv` contains:
- `asin` - Amazon Standard Identification Number
- `marketplaceId` - Marketplace ID
- `rank_type` - Type of ranking (classification or display_group)
- `title` - Product title
- `rank` - Sales rank
- `link` - Product link
- `classificationId` - Classification ID
- `websiteDisplayGroup` - Display group
- `extracted_at` - Extraction timestamp

# GET_LEDGER_SUMMARY_VIEW_DATA

Purpose: Aggregated financial summary over a period, akin to an inventory bank statement.

Usage:

```json
{"reportType":"GET_LEDGER_SUMMARY_VIEW_DATA","reportOptions":{...}}
```

ReportOptions:

- `aggregateByLocation`: COUNTRY (default) or FC.
- `aggregatedByTimePeriod`: MONTHLY (default), WEEKLY, DAILY.
- `FNSKU`: Filter to a specific fulfillment network SKU.
- `MSKU`: Filter to a merchant SKU.
- `ASIN`: Filter to an ASIN.

Report Attributes:

| Field                         | Description                                                  |
|-------------------------------|--------------------------------------------------------------|
| Date                          | Date boundary for the aggregated period.                     |
| FNSKU                         | Fulfillment network SKU identifier.                          |
| ASIN                          | Amazon Standard Identification Number.                       |
| MSKU                          | Merchant SKU mapped to FNSKU(s).                             |
| Title                         | Product name/title.                                          |
| Disposition                   | Final disposition (e.g., Sale, Return, Adjustment).          |
| StartingWarehouseBalance      | Inventory balance at period start.                           |
| InTransitBetweenWarehouses    | Quantity moving between warehouses.                         |
| Receipts                      | Total received units.                                        |
| CustomerShipments             | Units shipped to customers.                                  |
| CustomerReturns               | Units returned by customers.                                 |
| VendorReturns                 | Units returned by vendor.                                    |
| WarehouseTransferIn/Out       | Internal transfers in/out of warehouse.                      |
| Found, Lost, Damaged, Disposed, OtherEvents | Various adjustments.                      |
| EndingWarehouseBalance        | Inventory balance at period end.                             |
| UnknownEvents                 | Events not categorized.                                      |
| Location                      | COUNTRY or FC based on aggregation.                          |

# GET_LEDGER_DETAIL_VIEW_DATA

Purpose: Detailed, transaction-level ledger of inventory movements for the last 18 months.

Usage:

```json
{"reportType":"GET_LEDGER_DETAIL_VIEW_DATA","reportOptions":{...}}
```

ReportOptions:

- `eventType`: Filter for event types (Adjustments, CustomerReturns, Receipts, Shipments, VendorReturns, WhseTransfers). Default returns all.
- `FNSKU`: Filter to a specific fulfillment network SKU.
- `MSKU`: Filter to a merchant SKU.
- `ASIN`: Filter to an ASIN.

Report Attributes:

| Field                 | Description                                               |
|-----------------------|-----------------------------------------------------------|
| Date                  | Timestamp of the event.                                   |
| FNSKU                 | Fulfillment network SKU identifier.                       |
| ASIN                  | Amazon Standard Identification Number.                    |
| MSKU                  | Merchant SKU mapped to FNSKU(s).                          |
| Title                 | Product name/title.                                       |
| EventType             | Type of movement (Adjustments, Shipments, etc.).          |
| ReferenceID           | Amazon transaction/order ID reference.                    |
| Quantity              | Number of units in this event.                            |
| FulfillmentCenter     | ID of the FC where the event occurred.                    |
| Disposition           | Final disposition of the movement.                        |
| Reason                | Reason code for the event (e.g., damage, disposal).       |
| Country               | Country code of the FC.                                   |
| ReconciledQuantity    | Quantity successfully reconciled.                         |
| UnreconciledQuantity  | Quantity pending reconciliation.                          |
```

## Sample Configuration

```json
{
  "parameters": {
    "#refresh_token": "EncryptedToken",
    "#app_id": "EncryptedAppID", 
    "#client_secret_id": "EncryptedClientSecret",
    "#refresh_token_ads": "EncryptedTokenAds",
    "#app_id_ads": "EncryptedAppIDAds",
    "#client_secret_id_ads": "EncryptedClientSecretAds",
    "date_range": "30",
    "stores": [
      {
        "name": "Amazon.it",
        "scope": "1234567890"
      },
      {
        "name": "Amazon.de", 
        "scope": "0987654321"
      }
    ],
    "marketplaces": [
      {
        "marketplace_id": "APJ6JRA9NG5V4",
        "strategic_products": ["B08N5WRWNW", "B07H8QMZWV"]
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
}